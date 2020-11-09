<?php

namespace Keboola\OutputMapping\Tests;

use Keboola\InputMapping\Reader\NullWorkspaceProvider;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Tests\Writer\BaseWriterTest;
use Keboola\OutputMapping\Writer\FileWriter;
use Keboola\OutputMapping\Writer\TableWriter;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\Options\ListFilesOptions;
use Psr\Log\NullLogger;

class BranchStorageApiWriterTest extends BaseWriterTest
{
    public function setUp()
    {
        parent::setUp();
        $this->clearFileUploads(['output-mapping-bundle-test']);
        $this->clearBuckets([
            'out.c-output-mapping-test',
            'out.c-output-mapping-default-test',
            'out.c-output-mapping-redshift-test',
            'in.c-output-mapping-test',
        ]);
        $this->client->createBucket('output-mapping-default-test', 'out');

        $this->clearBranches();
    }

    public function testWriteFilesOutputMapping()
    {
        $branches = new DevBranches($this->client);
        $branch = $branches->createBranch($this->getBranchPrefix() . '\\' . $this->getName());

        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/file1", "test");

        $configs = [
            [
                "source" => "file1",
                "tags" => ["output-mapping-test"],
            ]
        ];

        $writer = new FileWriter($this->client, new NullLogger());

        try {
            $writer->uploadFiles(
                $root . "/upload",
                [
                    "mapping" => $configs,
                    'branchId' => $branch['id']
                ]
            );
            $this->fail('Should have thrown');
        } catch (InvalidOutputException $e) {

            $this->assertSame(400, $e->getCode());
            $this->assertSame(
                'File output mapping is not supported for dev branches',
                $e->getMessage()
            );
        }
        /*sleep(1);

        $options = new ListFilesOptions();
        $options->setTags(["output-mapping-test"]);
        $files = $this->client->listFiles($options);
        $this->assertCount(1, $files);

        $file1 = null;
        foreach ($files as $file) {
            if ($file["name"] == 'file1') {
                $file1 = $file;
            }
        }

        $this->assertNotNull($file1);
        $this->assertEquals(4, $file1["sizeBytes"]);
        $this->assertEquals(["output-mapping-test"], $file1["tags"]);*/
    }

    public function testWriteTableOutputMapping()
    {
        $branches = new DevBranches($this->client);
        $branch = $branches->createBranch($this->getBranchPrefix() . '\\' . $this->getName());

        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/table1a.csv", "\"Id\",\"Name\"\n\"test\",\"test\"\n\"aabb\",\"ccdd\"\n");
        file_put_contents($root . "/upload/table2a.csv", "\"Id2\",\"Name2\"\n\"test2\",\"test2\"\n\"aabb2\",\"ccdd2\"\n");

        $configs = [
            [
                "source" => "table1a.csv",
                "destination" => "out.c-output-mapping-test.table1a"
            ],
            [
                "source" => "table2a.csv",
                "destination" => "out.c-output-mapping-test.table2a"
            ]
        ];

        $writer = new TableWriter($this->client, new NullLogger(), new NullWorkspaceProvider());

        try {
            $tableQueue =  $writer->uploadTables(
                $root . "/upload",
                [
                    "mapping" => $configs,
                    'branchId' => $branch['id']
                ],
                [
                    'componentId'
                    =>
                        'foo'
                ],
                'local'
            );
            $this->fail('Should have thrown');
        } catch (InvalidOutputException $e) {
            $this->assertSame(400, $e->getCode());
            $this->assertSame(
                'Table output mapping is not supported for dev branches',
                $e->getMessage()
            );
        }
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(2, $jobIds);

        $tables = $this->client->listTables("out.c-output-mapping-test");
        $this->assertCount(2, $tables);
        $tableIds = [$tables[0]["id"], $tables[1]["id"]];
        sort($tableIds);
        $this->assertEquals(['out.c-output-mapping-test.table1a', 'out.c-output-mapping-test.table2a'], $tableIds);
        $this->assertCount(2, $jobIds);
        $this->assertNotEmpty($jobIds[0]);
        $this->assertNotEmpty($jobIds[1]);
    }

    public function testWriteTableOutputMappingExistingTable()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/table21.csv", "\"Id\",\"Name\"\n\"test\",\"test\"\n\"aabb\",\"ccdd\"\n");

        $configs = [
            [
                "source" => "table21.csv",
                "destination" => "out.c-output-mapping-test.table21"
            ]
        ];

        $writer = new TableWriter($this->client, new NullLogger(), new NullWorkspaceProvider());
        $tableQueue =  $writer->uploadTables($root . "/upload", ["mapping" => $configs], ['componentId' => 'foo'], 'local');
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        // And again
        $tableQueue =  $writer->uploadTables($root . "/upload", ["mapping" => $configs], ['componentId' => 'foo'], 'local');
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $tables = $this->client->listTables("out.c-output-mapping-test");
        $this->assertCount(1, $tables);
        $this->assertEquals('out.c-output-mapping-test.table21', $tables[0]["id"]);
        $this->assertNotEmpty($jobIds[0]);
    }

    private function clearBranches()
    {
        $branches = new DevBranches($this->client);
        foreach ($branches->listBranches() as $branch) {
            if ($branch['isDefault'] === true) {
                continue;
            }
            if (strpos($branch['name'], $this->getBranchPrefix()) !== 0) {
                continue;
            }
            $branches->deleteBranch($branch['id']);
        }
    }

    private function getBranchPrefix()
    {
        return __CLASS__;
    }
}
