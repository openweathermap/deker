"""Tests of the adapters factory interface."""

from concurrent.futures import ThreadPoolExecutor

import pytest

from deker_local_adapters import AdaptersFactory

from deker.ctx import CTX
from deker.uri import Uri


class TestBaseAdaptersFactoryClose:
    """``close`` shuts down the executor the factory owns and survives a factory that was never set up."""

    def test_close_of_a_factory_that_failed_to_initialize_does_not_raise(self):
        """``__del__`` calls ``close`` even when ``__init__`` raised before the executor was set."""
        factory = AdaptersFactory.__new__(AdaptersFactory)
        assert not hasattr(factory, "executor")
        assert factory.close() is None

    def test_close_shuts_down_an_own_executor(self, ctx: CTX, uri: Uri):
        own_ctx = CTX(uri=ctx.uri, config=ctx.config, executor=None, is_closed=False)
        factory = AdaptersFactory(own_ctx, uri)
        assert factory._own_executor is True
        executor = factory.executor
        factory.close()
        assert executor._shutdown is True
        with pytest.raises(RuntimeError):
            executor.submit(print)

    def test_close_keeps_a_borrowed_executor(self, ctx: CTX, uri: Uri):
        with ThreadPoolExecutor(max_workers=1) as executor:
            borrowed_ctx = CTX(uri=ctx.uri, config=ctx.config, executor=executor, is_closed=False)
            factory = AdaptersFactory(borrowed_ctx, uri)
            assert factory._own_executor is False
            factory.close()
            assert executor._shutdown is False
            assert executor.submit(sum, (1, 2)).result() == 3
