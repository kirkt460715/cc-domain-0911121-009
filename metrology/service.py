from .ledger.database import Database
from .ledger.catalog import Catalog
from .processing.pipeline import Pipeline
from .processing.windows import WindowEngine
from .output.reports import Reports
from .output.snapshots import Snapshots
from .configuration import Configuration
from .inspection import Inspector
from .processing.campaigns import Campaigns
from .processing.replay_plan import ReplayPlanner
from .output.bundles import EvidenceBundles


class Service:
    def __init__(self, path=":memory:"):
        self.database = Database(path)
        self.catalog = Catalog(self.database)
        self.pipeline = Pipeline(self.database, self.catalog)
        self.windows = WindowEngine(self.database, self.catalog)
        self.reports = Reports(self.database, self.catalog)
        self.snapshots = Snapshots(self.database, self.windows)
        self.configuration = Configuration(self.database, self.catalog)
        self.campaigns = Campaigns(self.database, self.catalog, self.reports)
        self.replay_plans = ReplayPlanner(self.database, self.pipeline, self.catalog)
        self.bundles = EvidenceBundles(self.snapshots, self.campaigns)
        self.inspector = Inspector(self.database)

    def close(self):
        self.database.close()

    def __enter__(self):
        return self

    def __exit__(self, kind, value, traceback):
        self.close()
