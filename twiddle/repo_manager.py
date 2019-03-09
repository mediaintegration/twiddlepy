from .repo_solr import RepositorySolr
from .repo_file import RepositoryCsv

class RepositoryManager:
    def __init__(self, config):
        self.config = config
        self.build_repository()

    def get_repository(self):
        return self.repository

    def build_repository(self):
        self.repo_type = self.config['DataRepository']['Type']
        self.repo_config_section = 'Repository' + self.repo_type.capitalize()

        if self.repo_type.lower() == 'solr':
            self.repository = RepositorySolr(self.config[self.repo_config_section])
        elif self.repo_type.lower() == 'csv':
            self.repository = RepositoryCsv(self.config[self.repo_config_section])