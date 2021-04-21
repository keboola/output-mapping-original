<?php

namespace Keboola\OutputMapping\Writer;

use Keboola\Csv\CsvFile;
use Keboola\Csv\Exception;
use Keboola\OutputMapping\DeferredTasks\LoadTable;
use Keboola\OutputMapping\DeferredTasks\MetadataDefinition;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Staging\StrategyFactory;
use Keboola\OutputMapping\Writer\Helper\PrimaryKeyHelper;
use Keboola\OutputMapping\Writer\Table\StrategyInterface;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\Temp\Temp;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use MicrosoftAzure\Storage\Blob\Models\ListBlobsOptions;
use Psr\Log\LoggerInterface;
use Symfony\Component\Finder\Finder;

class TableUploader
{
    const SYSTEM_METADATA_PROVIDER = 'system';
    const KBC_LAST_UPDATED_BY_BRANCH_ID = 'KBC.lastUpdatedBy.branch.id';
    const KBC_LAST_UPDATED_BY_CONFIGURATION_ROW_ID = 'KBC.lastUpdatedBy.configurationRow.id';
    const KBC_LAST_UPDATED_BY_CONFIGURATION_ID = 'KBC.lastUpdatedBy.configuration.id';
    const KBC_LAST_UPDATED_BY_COMPONENT_ID = 'KBC.lastUpdatedBy.component.id';
    const KBC_CREATED_BY_BRANCH_ID = 'KBC.createdBy.branch.id';
    const KBC_CREATED_BY_CONFIGURATION_ROW_ID = 'KBC.createdBy.configurationRow.id';
    const KBC_CREATED_BY_CONFIGURATION_ID = 'KBC.createdBy.configuration.id';
    const KBC_CREATED_BY_COMPONENT_ID = 'KBC.createdBy.component.id';

    /** @var ClientWrapper */
    private $clientWrapper;

    /** @var Metadata */
    private $metadataClient;

    /** @var LoggerInterface */
    private $logger;

    public function __construct(ClientWrapper $clientWrapper, Metadata $metadataClient, LoggerInterface $logger)
    {
        $this->clientWrapper = $clientWrapper;
        $this->metadataClient = $metadataClient;
        $this->logger = $logger;
    }

    /**
     * @param StrategyInterface $strategy
     * @param string $stagingStorageOutput
     * @param string $pathPrefix
     * @param string $source
     * @param array $config
     * @param array $systemMetadata
     * @return LoadTable
     * @throws ClientException
     */
    public function uploadTable(
        StrategyInterface $strategy,
        $stagingStorageOutput,
        $pathPrefix,
        $source,
        array $config,
        array $systemMetadata
    ) {
        if (is_dir($source) && empty($config['columns'])) {
            throw new InvalidOutputException(
                sprintf('Sliced file "%s" columns specification missing.', basename($source))
            );
        }
        if (!$this->clientWrapper->getBasicClient()->bucketExists($this->getBucketId($config['destination']))) {
            $this->createBucket($config['destination'], $systemMetadata);
        } else {
            $this->checkDevBucketMetadata($config['destination']);
        }

        if ($this->clientWrapper->getBasicClient()->tableExists($config['destination'])) {
            $tableInfo = $this->clientWrapper->getBasicClient()->getTable($config['destination']);
            PrimaryKeyHelper::validatePrimaryKeyAgainstTable($this->logger, $tableInfo, $config);
            if (PrimaryKeyHelper::modifyPrimaryKeyDecider($this->logger, $tableInfo, $config)) {
                PrimaryKeyHelper::modifyPrimaryKey(
                    $this->logger,
                    $this->clientWrapper->getBasicClient(),
                    $config['destination'],
                    $tableInfo['primaryKey'],
                    $config['primary_key']
                );
            }
            if (!empty($config['delete_where_column'])) {
                // Delete rows
                $deleteOptions = [
                    'whereColumn' => $config['delete_where_column'],
                    'whereOperator' => $config['delete_where_operator'],
                    'whereValues' => $config['delete_where_values'],
                ];
                $this->clientWrapper->getBasicClient()->deleteTableRows($config['destination'], $deleteOptions);
            }

        } else {
            $primaryKey = implode(",", PrimaryKeyHelper::normalizeKeyArray($this->logger, $config['primary_key']));
            $distributionKey = implode(
                ",",
                PrimaryKeyHelper::normalizeKeyArray($this->logger, $config['distribution_key'])
            );

            if (!empty($config['columns'])) {
                $this->createTable(
                    $config['destination'],
                    $config['columns'],
                    $primaryKey,
                    $distributionKey ?: null
                );

            } else {
                try {
                    $csvFile = new CsvFile($source, $config['delimiter'], $config['enclosure']);
                    $header = $csvFile->getHeader();
                } catch (Exception $e) {
                    throw new InvalidOutputException('Failed to read file ' . $source . ' ' . $e->getMessage());
                }
                $this->createTable(
                    $config['destination'],
                    $header,
                    $primaryKey,
                    $distributionKey ?: null
                );
                unset($csvFile);
            }
            $this->metadataClient->postTableMetadata(
                $config['destination'],
                self::SYSTEM_METADATA_PROVIDER,
                $this->getCreatedMetadata($systemMetadata)
            );
        }

        $loadOptions = [
            'delimiter' => $config['delimiter'],
            'enclosure' => $config['enclosure'],
            'columns' => !empty($config['columns']) ? $config['columns'] : [],
            'incremental' => $config['incremental'],
        ];

        $tableQueue = $this->loadDataIntoTable(
            $strategy,
            $stagingStorageOutput,
            $pathPrefix,
            $source,
            $config['destination'],
            $loadOptions
        );

        $tableQueue->addMetadata(new MetadataDefinition(
            $this->clientWrapper->getBasicClient(),
            $config['destination'],
            self::SYSTEM_METADATA_PROVIDER,
            $this->getUpdatedMetadata($systemMetadata),
            'table'
        ));
        return $tableQueue;
    }

    private function checkDevBucketMetadata($destination)
    {
        if (!$this->clientWrapper->hasBranch()) {
            return;
        }

        $bucketId = $this->getBucketId($destination);
        $metadata = new Metadata($this->clientWrapper->getBasicClient());
        try {
            foreach ($metadata->listBucketMetadata($bucketId) as $metadatum) {
                if (($metadatum['key'] === self::KBC_LAST_UPDATED_BY_BRANCH_ID) ||
                    ($metadatum['key'] === self::KBC_CREATED_BY_BRANCH_ID)) {
                    if ((string) $metadatum['value'] === (string) $this->clientWrapper->getBranchId()) {
                        return;
                    }

                    throw new InvalidOutputException(sprintf(
                        'Trying to create a table in the development bucket "%s" on branch ' .
                        '"%s" (ID "%s"). The bucket metadata marks it as assigned to branch with ID "%s".',
                        $bucketId,
                        $this->clientWrapper->getBranchName(),
                        $this->clientWrapper->getBranchId(),
                        $metadatum['value']
                    ));
                }
            }
        } catch (ClientException $e) {
            // this is Ok, if the bucket it does not exists, it can't have wrong metadata
            if ($e->getCode() === 404) {
                return;
            }

            throw $e;
        }

        throw new InvalidOutputException(sprintf(
            'Trying to create a table in the development ' .
            'bucket "%s" on branch "%s" (ID "%s"), but the bucket is not assigned to any development branch.',
            $bucketId,
            $this->clientWrapper->getBranchName(),
            $this->clientWrapper->getBranchId()
        ));
    }

    private function createBucket($tableId, array $systemMetadata)
    {
        // Create bucket if not exists
        $this->clientWrapper->getBasicClient()->createBucket($this->getBucketName($tableId), $this->getBucketStage($tableId));
        $this->metadataClient->postBucketMetadata(
            $this->getBucketId($tableId),
            self::SYSTEM_METADATA_PROVIDER,
            $this->getCreatedMetadata($systemMetadata)
        );
    }

    private function createTable($tableId, array $columns, $primaryKey, $distributionKey = null)
    {
        $tmp = new Temp();
        $headerCsvFile = new CsvFile($tmp->createFile($this->getTableName($tableId) . '.header.csv'));
        $headerCsvFile->writeRow($columns);
        $options = ['primaryKey' => $primaryKey];
        if (isset($distributionKey)) {
            $options['distributionKey'] = $distributionKey;
        }

        return $this->clientWrapper->getBasicClient()->createTableAsync(
            $this->getBucketId($tableId),
            $this->getTableName($tableId),
            $headerCsvFile,
            $options
        );
    }

    /**
     * @param StrategyInterface $strategy
     * @param string $stagingStorageOutput
     * @param string $pathPrefix
     * @param string $sourcePath
     * @param string $tableId
     * @param array $options
     * @return LoadTable
     * @throws ClientException
     */
    private function loadDataIntoTable(
        StrategyInterface $strategy,
        $stagingStorageOutput,
        $pathPrefix,
        $sourcePath,
        $tableId,
        array $options
    ) {
        if ($stagingStorageOutput === StrategyFactory::LOCAL) {
            if (is_dir($sourcePath)) {
                $fileId = $this->uploadSlicedFile($sourcePath);
            } else {
                $fileId = $this->clientWrapper->getBasicClient()->uploadFile(
                    $sourcePath,
                    (new FileUploadOptions())->setCompress(true)
                );
            }

            $options['dataFileId'] = $fileId;
            return new LoadTable($this->clientWrapper->getBasicClient(), $tableId, $options);
        }

        if ($stagingStorageOutput === StrategyFactory::WORKSPACE_ABS) {
            $sourcePath = $this->specifySourceAbsPath($strategy, $pathPrefix, $sourcePath);
        }

        $dataStorage = $strategy->getDataStorage();
        $options = [
            'dataWorkspaceId' => $dataStorage->getWorkspaceId(),
            'dataObject' => $sourcePath,
            'incremental' => $options['incremental'],
            'columns' => $options['columns'],
        ];

        return new LoadTable($this->clientWrapper->getBasicClient(), $tableId, $options);
    }

    /**
     * @param StrategyInterface $strategy
     * @param string $pathPrefix
     * @param string $sourcePath
     * @return string
     */
    private function specifySourceAbsPath(StrategyInterface $strategy, $pathPrefix, $sourcePath)
    {
        $path = $this->ensurePathDelimiter($pathPrefix) . $sourcePath;
        $absCredentials = $strategy->getDataStorage()->getCredentials();
        $blobClient = BlobRestProxy::createBlobService($absCredentials['connectionString']);
        try {
            $options = new ListBlobsOptions();
            $options->setPrefix($path);
            $blobs = $blobClient->listBlobs($absCredentials['container'], $options);
            $isSliced = false;
            foreach ($blobs->getBlobs() as $blob) {
                /* there can be multiple blobs with the same prefix (e.g `my`, `my-file`, ...), we're checking
                    if there are blobs where the prefix is a directory. (e.g `my/` or `my-file/`) */
                if (substr($blob->getName(), 0, strlen($path) + 1) === $path . '/') {
                    $isSliced = true;
                }
            }
            if ($isSliced) {
                $path .= '/';
            }
        } catch (\Exception $e) {
            throw new InvalidOutputException('Failed to list blobs ' . $e->getMessage(), 0, $e);
        }
        return $path;
    }

    protected function ensurePathDelimiter($path)
    {
        return $this->ensureNoPathDelimiter($path) . '/';
    }

    protected function ensureNoPathDelimiter($path)
    {
        return rtrim($path, '\\/');
    }

    /**
     * Uploads a sliced table to storage api. Takes all files from the $source folder
     *
     * @param string $source Slices folder
     * @return string
     * @throws ClientException
     */
    private function uploadSlicedFile($source)
    {
        $finder = new Finder();
        $slices = $finder->files()->in($source)->depth(0);
        $sliceFiles = [];
        /** @var \SplFileInfo $slice */
        foreach ($slices as $slice) {
            $sliceFiles[] = $slice->getPathname();
        }

        // upload slices
        $fileUploadOptions = new FileUploadOptions();
        $fileUploadOptions
            ->setIsSliced(true)
            ->setFileName(basename($source))
            ->setCompress(true);
        return $this->clientWrapper->getBasicClient()->uploadSlicedFile($sliceFiles, $fileUploadOptions);
    }

    /**
     * @param array $systemMetadata
     * @return array
     */
    private function getCreatedMetadata(array $systemMetadata)
    {
        $metadata[] = [
            'key' => self::KBC_CREATED_BY_COMPONENT_ID,
            'value' => $systemMetadata[AbstractWriter::SYSTEM_KEY_COMPONENT_ID],
        ];
        if (!empty($systemMetadata[AbstractWriter::SYSTEM_KEY_CONFIGURATION_ID])) {
            $metadata[] = [
                'key' => self::KBC_CREATED_BY_CONFIGURATION_ID,
                'value' => $systemMetadata[AbstractWriter::SYSTEM_KEY_CONFIGURATION_ID],
            ];
        }
        if (!empty($systemMetadata[AbstractWriter::SYSTEM_KEY_CONFIGURATION_ROW_ID])) {
            $metadata[] = [
                'key' => self::KBC_CREATED_BY_CONFIGURATION_ROW_ID,
                'value' => $systemMetadata[AbstractWriter::SYSTEM_KEY_CONFIGURATION_ROW_ID],
            ];
        }
        if (!empty($systemMetadata[AbstractWriter::SYSTEM_KEY_BRANCH_ID])) {
            $metadata[] = [
                'key' => self::KBC_CREATED_BY_BRANCH_ID,
                'value' => $systemMetadata[AbstractWriter::SYSTEM_KEY_BRANCH_ID],
            ];
        }
        return $metadata;
    }

    /**
     * @param array $systemMetadata
     * @return array
     */
    private function getUpdatedMetadata(array $systemMetadata)
    {
        $metadata[] = [
            'key' => self::KBC_LAST_UPDATED_BY_COMPONENT_ID,
            'value' => $systemMetadata[AbstractWriter::SYSTEM_KEY_COMPONENT_ID],
        ];
        if (!empty($systemMetadata[AbstractWriter::SYSTEM_KEY_CONFIGURATION_ID])) {
            $metadata[] = [
                'key' => self::KBC_LAST_UPDATED_BY_CONFIGURATION_ID,
                'value' => $systemMetadata[AbstractWriter::SYSTEM_KEY_CONFIGURATION_ID],
            ];
        }
        if (!empty($systemMetadata[AbstractWriter::SYSTEM_KEY_CONFIGURATION_ROW_ID])) {
            $metadata[] = [
                'key' => self::KBC_LAST_UPDATED_BY_CONFIGURATION_ROW_ID,
                'value' => $systemMetadata[AbstractWriter::SYSTEM_KEY_CONFIGURATION_ROW_ID],
            ];
        }
        if (!empty($systemMetadata[AbstractWriter::SYSTEM_KEY_BRANCH_ID])) {
            $metadata[] = [
                'key' => self::KBC_LAST_UPDATED_BY_BRANCH_ID,
                'value' => $systemMetadata[AbstractWriter::SYSTEM_KEY_BRANCH_ID],
            ];
        }
        return $metadata;
    }

    private function getBucketName($tableId)
    {
        return substr($this->getTableIdParts($tableId)[1], 2);
    }

    private function getBucketStage($tableId)
    {
        return $this->getTableIdParts($tableId)[0];
    }

    private function getBucketId($tableId)
    {
        $tableIdParts = $this->getTableIdParts($tableId);
        return $tableIdParts[0] . '.' . $tableIdParts[1];
    }

    private function getTableName($tableId)
    {
        return $this->getTableIdParts($tableId)[2];
    }

    private function getTableIdParts($tableId)
    {
        return explode('.', $tableId);
    }
}
