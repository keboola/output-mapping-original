<?php

namespace Keboola\OutputMapping\Writer;

use Keboola\OutputMapping\Configuration\Table\Manifest as TableManifest;
use Keboola\OutputMapping\Configuration\Table\Manifest\Adapter as TableAdapter;
use Keboola\OutputMapping\DeferredTasks\LoadTableQueue;
use Keboola\OutputMapping\DeferredTasks\MetadataDefinition;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Exception\OutputOperationException;
use Keboola\OutputMapping\Staging\StrategyFactory;
use Keboola\OutputMapping\Writer\Helper\ConfigurationMerger;
use Keboola\OutputMapping\Writer\Helper\DestinationRewriter;
use Keboola\OutputMapping\Writer\Helper\ManifestHelper;
use Keboola\OutputMapping\Writer\Helper\PrimaryKeyHelper;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Metadata;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Finder\Finder;
use Symfony\Component\Finder\SplFileInfo;

class TableWriter extends AbstractWriter
{
    /** @var TableUploader */
    private $tableUploader;

    public function __construct(StrategyFactory $strategyFactory)
    {
        parent::__construct($strategyFactory);

        $this->tableUploader = new TableUploader(
            $this->clientWrapper,
            new Metadata($this->clientWrapper->getBasicClient()),
            $this->logger
        );
    }

    /**
     * @param string $source
     * @param array $configuration
     * @param array $systemMetadata
     * @param string $stagingStorageOutput
     * @return LoadTableQueue
     * @throws \Exception
     */
    public function uploadTables($source, array $configuration, array $systemMetadata, $stagingStorageOutput)
    {
        if (empty($systemMetadata[self::SYSTEM_KEY_COMPONENT_ID])) {
            throw new OutputOperationException('Component Id must be set');
        }

        if ($stagingStorageOutput === StrategyFactory::LOCAL) {
            return $this->uploadTablesLocal($source, $configuration, $systemMetadata, $stagingStorageOutput);
        }

        return $this->uploadTablesWorkspace($source, $configuration, $systemMetadata, $stagingStorageOutput);
    }

    /**
     * @param string $source
     * @param array $configuration
     * @param array $systemMetadata
     * @return LoadTableQueue
     * @throws \Exception
     */
    private function uploadTablesWorkspace($source, array $configuration, array $systemMetadata, $stagingStorageOutput)
    {
        $strategy = $this->strategyFactory->getTableOutputStrategy($stagingStorageOutput);

        $finder = new Finder();
        /** @var SplFileInfo[] $files */
        $files = $finder->files()->name('*.manifest')->in($strategy->getMetadataStorage()->getPath() . '/' . $source)->depth(0);

        $sources = [];
        // add output mappings fom configuration
        if (isset($configuration['mapping'])) {
            foreach ($configuration['mapping'] as $mapping) {
                $sources[$mapping['source']] = false;
            }
        }

        // add manifest files
        foreach ($files as $file) {
            $sources[$file->getBasename('.manifest')] = $file;
        }

        $jobs = [];
        /** @var SplFileInfo|bool $manifest */
        foreach ($sources as $sourceName => $manifest) {
            $configFromMapping = [];
            $configFromManifest = [];
            if (isset($configuration['mapping'])) {
                foreach ($configuration['mapping'] as $mapping) {
                    if (isset($mapping['source']) && $mapping['source'] === $sourceName) {
                        $configFromMapping = $mapping;
                        unset($configFromMapping['source']);
                    }
                }
            }

            $prefix = isset($configuration['bucket']) ? ($configuration['bucket'] . '.') : '';

            if ($manifest !== false) {
                $configFromManifest = $this->readTableManifest($manifest->getPathname());
                if (empty($configFromManifest['destination']) && isset($configuration['bucket'])) {
                    $configFromManifest['destination'] = $this->createDestinationConfigParam(
                        $prefix,
                        $manifest->getBasename('.manifest')
                    );
                }
            }

            try {
                $mergedConfig = ConfigurationMerger::mergeConfigurations($configFromManifest, $configFromMapping);
                if (empty($mergedConfig['destination'])) {
                    throw new InvalidOutputException(sprintf('Failed to resolve destination for output table "%s".', $sourceName));
                }
                $parsedConfig = (new TableManifest())->parse([$mergedConfig]);
            } catch (InvalidConfigurationException $e) {
                throw new InvalidOutputException(
                    "Failed to write manifest for table {$sourceName}." . $e->getMessage(),
                    0,
                    $e
                );
            }

            try {
                $parsedConfig['primary_key'] =
                    PrimaryKeyHelper::normalizeKeyArray($this->logger, $parsedConfig['primary_key']);
                $parsedConfig = DestinationRewriter::rewriteDestination($parsedConfig, $this->clientWrapper);
                $tableJob = $this->tableUploader->uploadTable(
                    $strategy,
                    $stagingStorageOutput,
                    $source,
                    $sourceName,
                    $parsedConfig,
                    $systemMetadata
                );
            } catch (ClientException $e) {
                throw new InvalidOutputException(
                    "Cannot upload file '{$sourceName}' to table '{$parsedConfig["destination"]}' in Storage API: "
                    . $e->getMessage(),
                    $e->getCode(),
                    $e
                );
            }

            // After the file has been written, we can write metadata
            if (!empty($parsedConfig['metadata'])) {
                $tableJob->addMetadata(
                    new MetadataDefinition(
                        $this->clientWrapper->getBasicClient(),
                        $parsedConfig['destination'],
                        $systemMetadata[self::SYSTEM_KEY_COMPONENT_ID],
                        $parsedConfig['metadata'],
                        MetadataDefinition::TABLE_METADATA
                    )
                );
            }
            if (!empty($parsedConfig['column_metadata'])) {
                $tableJob->addMetadata(
                    new MetadataDefinition(
                        $this->clientWrapper->getBasicClient(),
                        $parsedConfig['destination'],
                        $systemMetadata[self::SYSTEM_KEY_COMPONENT_ID],
                        $parsedConfig['column_metadata'],
                        MetadataDefinition::COLUMN_METADATA
                    )
                );
            }
            $jobs[] = $tableJob;
        }

        $tableQueue = new LoadTableQueue($this->clientWrapper->getBasicClient(), $jobs);
        $tableQueue->start();
        return $tableQueue;
    }

    /**
     * @param string $source
     * @param array $configuration
     * @param array $systemMetadata
     * @param $stagingStorageOutput
     * @return LoadTableQueue
     * @throws \Exception
     */
    private function uploadTablesLocal($source, array $configuration, array $systemMetadata, $stagingStorageOutput)
    {
        $strategy = $this->strategyFactory->getTableOutputStrategy($stagingStorageOutput);

        if (empty($systemMetadata[self::SYSTEM_KEY_COMPONENT_ID])) {
            throw new OutputOperationException('Component Id must be set');
        }
        $manifestNames = ManifestHelper::getManifestFiles($strategy->getMetadataStorage()->getPath() . '/' . $source);

        $finder = new Finder();

        $outputMappingTables = [];
        if (isset($configuration['mapping'])) {
            foreach ($configuration['mapping'] as $mapping) {
                $outputMappingTables[] = $mapping['source'];
            }
        }
        $outputMappingTables = array_unique($outputMappingTables);
        $processedOutputMappingTables = [];

        /** @var SplFileInfo[] $files */
        $files = $finder->notName('*.manifest')->in($strategy->getDataStorage()->getPath() . '/' . $source)->depth(0);

        $fileNames = [];
        foreach ($files as $file) {
            $fileNames[] = $file->getFilename();
        }

        // Check if all files from output mappings are present
        if (isset($configuration['mapping'])) {
            foreach ($configuration['mapping'] as $mapping) {
                if (!in_array($mapping['source'], $fileNames)) {
                    throw new InvalidOutputException("Table source '{$mapping["source"]}' not found.", 404);
                }
            }
        }

        // Check for manifest orphans
        foreach ($manifestNames as $manifest) {
            if (!in_array(substr(basename($manifest), 0, -9), $fileNames)) {
                throw new InvalidOutputException("Found orphaned table manifest: '" . basename($manifest) . "'");
            }
        }

        $jobs = [];
        foreach ($files as $file) {
            $configFromMapping = [];
            $configFromManifest = [];
            if (isset($configuration['mapping'])) {
                foreach ($configuration['mapping'] as $mapping) {
                    if (isset($mapping['source']) && $mapping['source'] === $file->getFilename()) {
                        $configFromMapping = $mapping;
                        $processedOutputMappingTables[] = $configFromMapping['source'];
                        unset($configFromMapping['source']);
                    }
                }
            }

            $prefix = isset($configuration['bucket']) ? ($configuration['bucket'] . '.') : '';

            $manifestKey = array_search($file->getPathname() . '.manifest', $manifestNames);
            if ($manifestKey !== false) {
                $configFromManifest = $this->readTableManifest($file->getPathname() . '.manifest');
                if (empty($configFromManifest['destination']) || isset($configuration['bucket'])) {
                    $configFromManifest['destination'] = $this->createDestinationConfigParam(
                        $prefix,
                        $file->getFilename()
                    );
                }
                unset($manifestNames[$manifestKey]);
            } else {
                // If no manifest found and no output mapping, use filename (without .csv if present) as table id
                if (empty($configFromMapping['destination']) || isset($configuration['bucket'])) {
                    $configFromMapping['destination'] = $this->createDestinationConfigParam(
                        $prefix,
                        $file->getFilename()
                    );
                }
            }

            try {
                // Mapping with higher priority
                if ($configFromMapping || !$configFromManifest) {
                    $config = (new TableManifest())->parse([$configFromMapping]);
                } else {
                    $config = (new TableManifest())->parse([$configFromManifest]);
                }
            } catch (InvalidConfigurationException $e) {
                throw new InvalidOutputException(
                    "Failed to write manifest for table {$file->getFilename()}.",
                    0,
                    $e
                );
            }

            if (count(explode('.', $config['destination'])) !== 3) {
                throw new InvalidOutputException(sprintf(
                    'CSV file "%s" file name is not a valid table identifier, either set output mapping for ' .
                        '"%s" or make sure that the file name is a valid Storage table identifier.',
                    $config['destination'],
                    $file->getRelativePathname()
                ));
            }

            try {
                $config['primary_key'] = PrimaryKeyHelper::normalizeKeyArray($this->logger, $config['primary_key']);
                $config = DestinationRewriter::rewriteDestination($config, $this->clientWrapper);
                $tableJob = $this->tableUploader->uploadTable(
                    $strategy,
                    $stagingStorageOutput,
                    $source,
                    $file->getPathname(),
                    $config,
                    $systemMetadata
                );
            } catch (ClientException $e) {
                throw new InvalidOutputException(
                    "Cannot upload file '{$file->getFilename()}' to table '{$config["destination"]}' in Storage API: "
                    . $e->getMessage(),
                    $e->getCode(),
                    $e
                );
            }

            // After the file has been written, we can write metadata
            if (!empty($config['metadata'])) {
                $tableJob->addMetadata(
                    new MetadataDefinition(
                        $this->clientWrapper->getBasicClient(),
                        $config['destination'],
                        $systemMetadata[self::SYSTEM_KEY_COMPONENT_ID],
                        $config['metadata'],
                        MetadataDefinition::TABLE_METADATA
                    )
                );
            }
            if (!empty($config['column_metadata'])) {
                $tableJob->addMetadata(
                    new MetadataDefinition(
                        $this->clientWrapper->getBasicClient(),
                        $config['destination'],
                        $systemMetadata[self::SYSTEM_KEY_COMPONENT_ID],
                        $config['column_metadata'],
                        MetadataDefinition::COLUMN_METADATA
                    )
                );
            }
            $jobs[] = $tableJob;
        }

        $processedOutputMappingTables = array_unique($processedOutputMappingTables);
        $diff = array_diff(
            array_merge($outputMappingTables, $processedOutputMappingTables),
            $processedOutputMappingTables
        );
        if (count($diff)) {
            throw new InvalidOutputException(
                sprintf('Can not process output mapping for file(s): %s.', implode('", "', $diff))
            );
        }
        $tableQueue = new LoadTableQueue($this->clientWrapper->getBasicClient(), $jobs);
        $tableQueue->start();
        return $tableQueue;
    }

    /**
     * @param $source
     * @return array
     * @throws \Exception
     */
    private function readTableManifest($source)
    {
        $adapter = new TableAdapter($this->format);
        $fs = new Filesystem();
        if (!$fs->exists($source)) {
            throw new InvalidOutputException("File '$source' not found.");
        }
        try {
            $fileHandler = new SplFileInfo($source, "", basename($source));
            $serialized = $fileHandler->getContents();
            return $adapter->deserialize($serialized);
        } catch (InvalidConfigurationException $e) {
            throw new InvalidOutputException(
                'Failed to read table manifest from file ' . basename($source) . ' ' . $e->getMessage(),
                0,
                $e
            );
        }
    }

    /**
     * Creates destination configuration parameter from prefix and file name
     * @param $prefix
     * @param $filename
     * @return string
     */
    private function createDestinationConfigParam($prefix, $filename)
    {
        if (substr($filename, -4) === '.csv') {
            return $prefix . substr($filename, 0, -4);
        }

        return $prefix . $filename;
    }

}
