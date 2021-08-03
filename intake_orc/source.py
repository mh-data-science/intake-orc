from intake.source import base
from . import __version__


class ORCSource(base.DataSource):
    """
    Source to load orc datasets.

    Produces a dataframe.

    The implementation uses pyarrow.

    Keyword parameters accepted by this Source:

    - columns: list of str or None
        column names to load. If None, loads all

    - see dd.read_orc() for the other named parameters that can be passed through.
    """
    container = 'dataframe'
    name = 'orc'
    version = __version__
    partition_access = True

    def __init__(self, urlpath, metadata=None,
                 storage_options=None, **orc_kwargs):
        self._urlpath = urlpath
        self._storage_options = storage_options or {}
        self._kwargs = orc_kwargs or {}
        self._df = None

        super(ORCSource, self).__init__(metadata=metadata)

    def _get_schema(self):
        if self._df is None:
            self._df = self._to_dask()
        dtypes = {k: str(v) for k, v in self._df._meta.dtypes.items()}
        self._schema = base.Schema(datashape=None,
                                   dtype=dtypes,
                                   shape=(None, len(self._df.columns)),
                                   npartitions=self._df.npartitions,
                                   extra_metadata={})
        return self._schema

    def _get_partition(self, i):
        self._get_schema()
        return self._df.get_partition(i).compute()

    def read(self):
        """
        Create single pandas dataframe from the whole data-set
        """
        self._load_metadata()
        return self._df.compute()

    def to_spark(self):
        """Produce Spark DataFrame equivalent

        This will ignore all arguments except the urlpath, which will be
        directly interpreted by Spark. If you need to configure the storage,
        that must be done on the spark side.

        This method requires intake-spark. See its documentation for how to
        set up a spark Session.
        """
        raise NotImplementedError()

    def to_dask(self):
        self._load_metadata()
        return self._df

    def _to_dask(self):
        """
        Create a lazy dask-dataframe from the parquet data
        """
        import dask.dataframe as dd
        urlpath = self._get_cache(self._urlpath)[0]
        self._df = dd.read_orc(urlpath,
                               storage_options=self._storage_options, 
                               **self._kwargs)
        self._load_metadata()
        return self._df

    def _close(self):
        self._df = None
