class FakeConfigLoader:
    def __init__(self, pipeline_config):
        self.pipeline_config = pipeline_config

    def load_pipeline_config(self):
        return self.pipeline_config


class FakeQualityReporter:
    def __init__(self):
        self.calls = []

    def create_data_quality_report(self, **kwargs):
        self.calls.append(kwargs)
        return {"summary": kwargs}


class FakeRawStorageSaver:
    def __init__(self):
        self.calls = []

    def save_files_to_raw_storage(self, _config, _files, failed=False):
        self.calls.append({"failed": failed})


class FakeRelocator:
    def __init__(self, relocation_result):
        self.relocation_result = relocation_result
        self.calls = []

    def relocate_staged_trusted_files(self, _config, staged_results, target):
        self.calls.append((target, staged_results))
        return self.relocation_result


class FakeExtractor:
    def __init__(self, extracted_files):
        self.extracted_files = extracted_files

    def extract_gtfs_files(self, _config):
        return self.extracted_files


class FakeRawValidator:
    def __init__(self, validation_result):
        self.validation_result = validation_result

    def validate_raw_gtfs_files(self, _config, _files):
        return self.validation_result


class FakeTableTransformer:
    def __init__(self, results_by_table):
        self.results_by_table = results_by_table

    def transform_and_validate_table(self, _config, table_name):
        return self.results_by_table[table_name]


class FakeExtractLoadDependencies:
    def __init__(self, pipeline_config, extracted_files, validation_result):
        self.config_loader = FakeConfigLoader(pipeline_config)
        self.extractor = FakeExtractor(extracted_files)
        self.raw_validator = FakeRawValidator(validation_result)
        self.raw_storage_saver = FakeRawStorageSaver()
        self.quality_reporter = FakeQualityReporter()

    def load_pipeline_config(self):
        return self.config_loader.load_pipeline_config()

    def extract_gtfs_files(self, _config):
        return self.extractor.extract_gtfs_files(_config)

    def validate_raw_gtfs_files(self, _config, _files):
        return self.raw_validator.validate_raw_gtfs_files(_config, _files)

    def save_files_to_raw_storage(self, _config, _files, failed=False):
        self.raw_storage_saver.save_files_to_raw_storage(_config, _files, failed=failed)

    def create_data_quality_report(self, **kwargs):
        return self.quality_reporter.create_data_quality_report(**kwargs)

    @property
    def raw_save_calls(self):
        return self.raw_storage_saver.calls

    @property
    def quality_report_calls(self):
        return self.quality_reporter.calls


class FakeTransformationDependencies:
    def __init__(self, pipeline_config, results_by_table, relocation_result):
        self.config_loader = FakeConfigLoader(pipeline_config)
        self.table_transformer = FakeTableTransformer(results_by_table)
        self.relocator = FakeRelocator(relocation_result)
        self.quality_reporter = FakeQualityReporter()

    def load_pipeline_config(self):
        return self.config_loader.load_pipeline_config()

    def transform_and_validate_table(self, _config, table_name):
        return self.table_transformer.transform_and_validate_table(_config, table_name)

    def relocate_staged_trusted_files(self, _config, staged_results, target):
        return self.relocator.relocate_staged_trusted_files(_config, staged_results, target)

    def create_data_quality_report(self, **kwargs):
        return self.quality_reporter.create_data_quality_report(**kwargs)

    @property
    def relocation_calls(self):
        return self.relocator.calls

    @property
    def quality_report_calls(self):
        return self.quality_reporter.calls
