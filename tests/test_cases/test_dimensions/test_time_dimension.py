from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from deker.dimensions import TimeDimension
from deker.errors import DekerValidationError


class TestTimeDimensionStartValueValidation:
    @pytest.mark.parametrize(
        "start_value",
        [
            datetime.now(ZoneInfo("UTC")),
            datetime.utcnow().replace(tzinfo=ZoneInfo("UTC")),
            datetime.fromisoformat("2022-11-18T05:04:01.396756+11:00"),
            datetime.fromisoformat("2022-11-18T05:04:01.396756-11:00"),
            "2022-11-18T05:04:01.396756+10:00",
            "2022-11-18T05:04:01.396756-10:00",
            "2022-11-18T05:04:01.396756+00:00",
            "2022-11-18T05:04:01.396756-00:00",
        ],
    )
    def test_time_dimension_start_value_ok(self, start_value):
        assert TimeDimension(name="td", size=2, start_value=start_value, step=timedelta(weeks=1))

    @pytest.mark.parametrize(
        "start_value",
        [
            datetime.now(),
            datetime.utcnow(),
            datetime.fromisoformat("2022-11-18T05:04:01.396756"),
            datetime.fromisoformat("2022-11-18T05:04:01.396756"),
            datetime.fromtimestamp(1668747885.032391),
            datetime.fromtimestamp(1668747885),
            datetime.utcfromtimestamp(1668747885.032391),
            datetime.utcfromtimestamp(1668747885),
        ],
    )
    def test_time_dimension_start_value_raises_no_tzinfo(self, start_value):
        with pytest.raises(DekerValidationError):
            assert TimeDimension(
                name="td", size=2, start_value=start_value, step=timedelta(weeks=1)
            )

    @pytest.mark.parametrize(
        "start_value",
        [
            "",
            " ",
            "            ",
            "2022-11-18T05:04:01.396756",
            0,
            1,
            -1,
            1668747885,
            1668747885.032391,
            -1668747885,
            -1668747885.032391,
            [],
            tuple(),
            set(),
            {},
            [datetime.now(ZoneInfo("UTC"))],
            (datetime.now(ZoneInfo("UTC")),),
            {datetime.now(ZoneInfo("UTC"))},
            {"start_value": datetime.now(ZoneInfo("UTC"))},
            None,
            True,
            False,
        ],
    )
    def test_time_dimension_start_value_raises_invalid_type(self, start_value):
        with pytest.raises(DekerValidationError):
            assert TimeDimension(
                name="td", size=2, start_value=start_value, step=timedelta(weeks=1)
            )


class TestTimeDimensionStepValidation:
    @pytest.mark.parametrize(
        "step",
        [
            timedelta(weeks=1),
            timedelta(weeks=-1),
            timedelta(days=1),
            timedelta(days=-1),
            timedelta(hours=1),
            timedelta(hours=-1),
            timedelta(minutes=1),
            timedelta(minutes=-1),
            timedelta(seconds=1),
            timedelta(seconds=-1),
            timedelta(milliseconds=1),
            timedelta(milliseconds=-1),
            timedelta(microseconds=1),
            timedelta(microseconds=-1),
        ],
    )
    def test_time_dimension_step_ok(self, step):
        assert TimeDimension(
            name="td",
            size=2,
            start_value=datetime.now(ZoneInfo("UTC")),
            step=step,
        )

    @pytest.mark.parametrize(
        "step",
        [
            timedelta(weeks=0),
            timedelta(days=0),
            timedelta(hours=0),
            timedelta(minutes=0),
            timedelta(seconds=0),
            timedelta(milliseconds=0),
            timedelta(microseconds=0),
            dict(weeks=0),
            dict(days=0),
            dict(hours=0),
            dict(minutes=0),
            dict(seconds=0),
            dict(milliseconds=0),
            dict(microseconds=0),
            {},
            {"step": 1},
            {"step": {"hours": 1}},
            {"weeks": 1},
            {"weeks": -1},
            {"days": 1},
            {"days": -1},
            {"hours": 1},
            {"hours": -1},
            {"minutes": 1},
            {"minutes": -1},
            {"seconds": 1},
            {"seconds": -1},
            {"milliseconds": 1},
            {"milliseconds": -1},
            {"microseconds": 1},
            {"microseconds": -1},
        ],
    )
    def test_time_dimension_invalid_step_mapping_raises(self, step):
        with pytest.raises(DekerValidationError):
            assert TimeDimension(
                name="td",
                size=2,
                start_value=datetime.now(ZoneInfo("UTC")),
                step=step,
            )

    @pytest.mark.parametrize(
        "step",
        [
            None,
            True,
            False,
            "",
            " ",
            "            ",
            "2022-11-18T05:04:01.396756",
            "2022-11-18T05:04:01.396756+10:00",
            0,
            1,
            -1,
            2,
            -2,
            100,
            -100,
            2**128,
            -(2**128),
            0.1,
            -0.1,
            complex(0.00000000000000001),
            complex(-0.000000000001),
            1668747885.032391,
            -1668747885.032391,
            [],
            tuple(),
            set(),
            [1],
            (1,),
            {1},
        ],
    )
    def test_time_dimension_step_raises_invalid_type(self, step):
        with pytest.raises(DekerValidationError):
            assert TimeDimension(
                name="td",
                size=2,
                start_value=datetime.now(ZoneInfo("UTC")),
                step=step,
            )


if __name__ == "__main__":
    pytest.main()
