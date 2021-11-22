<?php

namespace Keboola\OutputMapping\Tests;

use Keboola\InputMapping\Table\Result\TableInfo;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Exception\OutputOperationException;
use Keboola\OutputMapping\Staging\StrategyFactory;
use Keboola\OutputMapping\Tests\Writer\BaseWriterTest;
use Keboola\OutputMapping\Tests\Writer\CreateBranchTrait;
use Keboola\OutputMapping\Writer\TableWriter;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApi\TableExporter;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\NullLogger;
use Psr\Log\Test\TestLogger;

class SynapseFixedTypesLocalTableWriterTest extends BaseWriterTest
{
    use InitSynapseStorageClientTrait;

    public function setUp()
    {
        if (!$this->checkSynapseTests()) {
            self::markTestSkipped('Synapse tests disabled.');
        }
        parent::setUp();
        $this->clearFileUploads(['output-mapping-bundle-test']);
        try {
            $this->clientWrapper->getBasicClient()->dropBucket(
                'in.c-output-mapping-synapse-test',
                ['force' => true]
            );
            $this->clientWrapper->getBasicClient()->dropBucket(
                'out.c-output-mapping-synapse-test',
                ['force' => true]
            );
        } catch (ClientException $e) {
            if ($e->getCode() != 404) {
                throw $e;
            }
        }
        $this->clientWrapper->getBasicClient()->createBucket('output-mapping-synapse-test', 'in');
        $this->clientWrapper->getBasicClient()->createBucket('output-mapping-synapse-test', 'out');
    }

    protected function initClient($branchId = '')
    {
        $this->clientWrapper = $this->getSynapseClientWrapper();
    }

    public function testWriteTableOutputMappingFixedTypesInvalid()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents(
            $root . "/upload/table1a.csv",
            "\"id\",\"name\"\n\"1\",\"test\"\n\"\",\"ccdd\"\n\"3\",\"\"\n"
        );

        // we will create the table on storage with fixed type definitions
        // https://keboola.docs.apiary.io/#reference/tables/create-table-definition/create-new-table-definition
        $this->clientWrapper->getBasicClient()->createTableDefinition(
            'out.c-output-mapping-synapse-test',
            [
                'name' => 'fixed-types-table',
                'primaryKeysNames' => ['id'],
                'columns' => [
                    [
                        'name' => 'id',
                        'definition' => [
                            'type' => 'INT',
                            'nullable' => false
                        ],
                    ], [
                        'name' => 'name',
                        'definition' => [
                            'type' => 'NVARCHAR',
                            'nullable' => false,
                        ],
                    ],
                ],
                'distribution' => [
                    'type' => 'HASH',
                    'distributionColumnsNames' => ['id'],
                ],
                'index' => [
                    'type' => 'CLUSTERED INDEX',
                    'indexColumnsNames' => ['id'],
                ]
            ]
        );
        $configs = [
            [
                "source" => "table1a.csv",
                "destination" => "out.c-output-mapping-synapse-test.fixed-types-table",
                "primary_key" => ['id'],
                "incremental" => true
            ]
        ];

        $writer = new TableWriter($this->getStagingFactory());

        $this->expectException(InvalidOutputException::class);
        $tableQueue = $writer->uploadTables('upload', ["mapping" => $configs], ['componentId' => 'foo'], 'local');
        $tableQueue->waitForAll();
    }
}
