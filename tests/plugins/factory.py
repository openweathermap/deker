import pytest

from deker_local_adapters.factory import AdaptersFactory


@pytest.fixture()
def factory(ctx, uri) -> AdaptersFactory:
    """Returns AdaptersFactory instance."""
    return AdaptersFactory(ctx, uri)
