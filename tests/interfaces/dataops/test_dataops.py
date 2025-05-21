import pytest
import abc

from typing import Protocol


class DataOpsProtocol(Protocol):
    def validate(self, data, config): ...


class BaseDataOpsTests(abc.ABC):
    """Abstract base class for testing dataops adapters."""

    @abc.abstractmethod
    def get_adapter(self) -> DataOpsProtocol:
        """Return the adapter implementation to test."""
        raise NotImplementedError

    # def test_validate(self, data, config, expected_result):
    #    adapter = self.get_adapter()
    #    return adapter.validate(data, config)


@pytest.mark.dataframe
class TestDataFrameDataOps(BaseDataOpsTests):
    def get_adapter(self) -> DataOpsProtocol:
        try:
            from pypeh.dataframe_adapter import dataops as dfops

            return dfops.DataOpsAdapter()
        except ImportError:
            pytest.skip("Necessary modules not installed")
