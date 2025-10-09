import math

from framcore import Base

# TODO: Storage period in clearing must go up in storage period in short. give error if not
class JulESTimeResolution(Base):
    """Time resolution settings for JulES (only some are modifiable)."""

    def __init__(self) -> None:
        """Create instance with default values."""
        self._target_med_days = 12 * 7
        self._target_long_storage_days = 6 * 7
        self._target_lookahead_days = 365 * 5
        self._target_ev_days = 365 * 3

        self._clearing_days = 2
        self._short_days = 5

        self._clearing_market_minutes = 60 * 3
        self._clearing_storage_minutes = 60 * 24

        self._short_market_minutes = self._get_incremented_divisor(
            n=self._clearing_days * 24 * 60,
            divisor=self._clearing_market_minutes,
            num_increments=1,
        )
        self._short_storage_minutes = 60 * 24 * self._short_days

        self._long_adaptive_blocks = 4
        self._long_adaptive_hours = 6

        self._med_adaptive_blocks = 4
        self._med_adaptive_hours = 6

        med_days, long_days, med_storage_days, long_storage_days = self._get_med_long_days_and_storage_days(
            self._target_lookahead_days,
            self._clearing_days,
            self._short_days,
        )

        self._med_days = med_days
        self._long_days = long_days
        self._med_storage_days = med_storage_days
        self._long_storage_days = long_storage_days

    def set_target_ev_days(self, x: int) -> None:
        """Set prefered value for length in days of end value problems.

        Will choose a close valid value.
        """
        self._check_type(x, int)
        self._check_int(value=x, lower_bound=self._clearing_days + self._short_days, upper_bound=None)
        self._target_ev_days = x

    def set_target_long_storage_days(self, x: int) -> None:
        """Set prefered value for long_storage_days.

        Will choose a close valid value.
        """
        self._check_type(x, int)
        self._check_int(value=x, lower_bound=self._clearing_days + self._short_days, upper_bound=None)
        self._target_long_storage_days = x

    def set_target_med_days(self, x: int) -> None:
        """Set prefered value for horizon length in days in medium prognosis problem.

        Will choose a close valid value.
        """
        self._check_type(x, int)
        self._check_int(value=x, lower_bound=self._clearing_days + self._short_days, upper_bound=None)
        self._target_med_num_period = x

    def set_clearing_days(self, x: int) -> None:
        """Set length of clearing problem in days."""
        self._check_type(x, int)
        self._check_int(value=x, lower_bound=1, upper_bound=None)

        self._clearing_days = x

        clearing_minutes = self._clearing_days * 24 * 60

        self._clearing_market_minutes = min(self._clearing_market_minutes, clearing_minutes)
        self._clearing_storage_minutes = min(self._clearing_storage_minutes, clearing_minutes)

        if clearing_minutes % self._clearing_market_minutes != 0:
            message = (
                f"clearing_market_minutes ({self._clearing_market_minutes}) does not go up"
                f" in clearing_days in minutes ({clearing_minutes})."
            )
            raise ValueError(message)

        if clearing_minutes % self._clearing_storage_minutes != 0:
            message = (
                f"clearing_storage_minutes ({self._clearing_storage_minutes}) does not go up"
                f" in clearing_days in minutes ({clearing_minutes})."
            )
            raise ValueError(message)

        self.set_target_lookahead_days(self._target_lookahead_days)

    def set_short_days(self, x: int) -> None:
        """Set length of short term prognosis problem in days."""
        self._check_type(x, int)
        self._check_int(value=x, lower_bound=1, upper_bound=None)

        self._short_days = x

        short_minutes = self._short_days * 24 * 60

        self._short_market_minutes = min(self._short_market_minutes, short_minutes)
        self._short_storage_minutes = min(self._short_storage_minutes, short_minutes)

        if short_minutes % self._short_market_minutes != 0:
            message = (
                f"short_market_minutes ({self._short_market_minutes}) does not go up"
                f" in short_days in minutes ({short_minutes})."
            )
            raise ValueError(message)

        if short_minutes % self._short_storage_minutes != 0:
            message = (
                f"short_storage_minutes ({self._short_storage_minutes}) does not go up"
                f" in short_days in minutes ({short_minutes})."
            )
            raise ValueError(message)

        self.set_target_lookahead_days(self._target_lookahead_days)

    def set_target_lookahead_days(self, x: int) -> None:
        """Set target length of prognosis problems in days.

        Will set med_days and long_days and make sure their sum is minimum this length.

        Will set short_days if target_lookahead_days < short_days.
        """
        self._check_type(x, int)
        self._check_int(value=x, lower_bound=1, upper_bound=None)

        self._target_lookahead_days = x

        if x < self._short_days:
            self.set_short_days(x)

        med_days, long_days, med_storage_days, long_storage_days = self._get_med_long_days_and_storage_days(
            self._target_lookahead_days,
            self._clearing_days,
            self._short_days,
        )

        self._med_days = med_days
        self._long_days = long_days
        self._med_storage_days = med_storage_days
        self._long_storage_days = long_storage_days

    def set_clearing_market_minutes(self, x: int) -> None:
        """Set market period length in clearing problem in minutes. Currently only support whole hours."""
        self._check_type(x, int)
        self._check_int(value=x, lower_bound=1, upper_bound=None)
        self._check_hourly(x)
        self._clearing_market_minutes = x

    def set_clearing_storage_minutes(self, x: int) -> None:
        """Set storage period length in clearing problem in minutes. Currently only support whole hours."""
        self._check_type(x, int)
        self._check_int(value=x, lower_bound=1, upper_bound=None)
        self._clearing_storage_minutes = x

    def set_short_market_minutes(self, x: int) -> None:
        """Set market period length in short prognosis problem in minutes. Currently only support whole hours."""
        self._check_type(x, int)
        self._check_int(value=x, lower_bound=1, upper_bound=None)
        self._short_market_minutes = x

    def set_short_storage_minutes(self, x: int) -> None:
        """Set storage period length in short prognosis problem in minutes. Currently only support whole hours."""
        self._check_type(x, int)
        self._check_int(value=x, lower_bound=1, upper_bound=None)
        self._short_storage_minutes = x

    def get_clearing_days(self) -> int:
        """Get length of clearing problem in days."""
        return self._clearing_days

    def get_short_days(self) -> int:
        """Get length of short prognosis problem in days."""
        return self._short_days

    def get_target_lookahead_days(self) -> int:
        """Get target (minimum) length of prognosis problems in days."""
        return self._target_lookahead_days

    def get_med_days(self) -> int:
        """Get length of medium prognosis problem in days."""
        return self._med_days

    def get_long_days(self) -> int:
        """Get length of long prognosis problem in days."""
        return self._long_days

    def get_med_storage_days(self) -> int:
        """Get storage period length in medium prognosis problem in days."""
        return self._med_storage_days

    def get_long_storage_days(self) -> int:
        """Get storage period length in long prognosis problem in days."""
        return self._long_storage_days

    def get_clearing_market_minutes(self) -> int:
        """Get market period length in clearing problem in minutes. Currently only support whole hours."""
        return self._clearing_market_minutes

    def get_clearing_storage_minutes(self) -> int:
        """Get storage period length in clearing problem in minutes. Currently only support whole hours."""
        return self._clearing_storage_minutes

    def get_short_market_minutes(self) -> int:
        """Get market period length in short prognosis problem in minutes. Currently only support whole hours."""
        return self._short_market_minutes

    def get_short_storage_minutes(self) -> int:
        """Get storage period length in short prognosis problem in minutes. Currently only support whole hours."""
        return self._short_storage_minutes

    def get_long_adaptive_blocks(self) -> int:
        """Get number of market periods long prognosis problem."""
        return self._long_adaptive_blocks

    def get_long_adaptive_hours(self) -> int:
        """Get resolution in hours used in clustering of market period blocks in medium prognosis problem."""
        return self._long_adaptive_hours

    def get_med_adaptive_blocks(self) -> int:
        """Get number of market periods long prognosis problem."""
        return self._med_adaptive_blocks

    def get_med_adaptive_hours(self) -> int:
        """Get resolution in hours used in clustering of market period blocks in medium prognosis problem."""
        return self._med_adaptive_hours

    def get_target_long_storage_days(self) -> int:
        """Get prefered value for long_storage_days."""
        return self._target_long_storage_days

    def get_target_med_days(self) -> int:
        """Get prefered value for horizon length of medium prognosis problem."""
        return self._target_med_days

    def get_target_ev_days(self) -> int:
        """Get prefered value for horizon length of end value problem."""
        return self._target_ev_days

    def get_ev_days(self) -> int:
        """Get number of days in horizon of end value problems."""
        target_horizon_days = self.get_target_ev_days()
        long_period_days = self.get_long_storage_days()
        return math.ceil(target_horizon_days / long_period_days) * long_period_days

    def get_content_dict(self) -> dict[str, int]:
        """Return dict of all settings. Useful to get an overview."""
        return {
            "clearing_days": self.get_clearing_days(),
            "short_days": self.get_short_days(),
            "med_days": self.get_med_days(),
            "long_days": self.get_long_days(),
            "long_storage_days": self.get_long_storage_days(),
            "med_storage_days": self.get_med_storage_days(),
            "clearing_market_minutes": self.get_clearing_market_minutes(),
            "clearing_storage_minutes": self.get_clearing_storage_minutes(),
            "short_market_minutes": self.get_short_market_minutes(),
            "short_storage_minutes": self.get_short_storage_minutes(),
            "long_adaptive_blocks": self.get_long_adaptive_blocks(),
            "long_adaptive_hours": self.get_long_adaptive_hours(),
            "med_adaptive_blocks": self.get_med_adaptive_blocks(),
            "med_adaptive_hours": self.get_med_adaptive_hours(),
            "target_lookahead_days": self.get_target_lookahead_days(),
            "target_long_storage_days": self.get_target_long_storage_days(),
            "target_med_days": self.get_target_med_days(),
            "target_ev_days": self.get_target_ev_days(),
            "ev_days": self.get_ev_days(),
        }

    def _get_med_long_days_and_storage_days(
        self,
        target_lookahead_days: int,
        clearing_days: int,
        short_days: int,
    ) -> tuple[int, int, int, int]:
        """Find the valid configuration that is closest to the user supplied targets."""
        # TODO: Remove restriction in Julia-JulES and do more candidates here (e.g. small variation around target)
        med_period = clearing_days + short_days

        possible_med_pairs = self._get_possible_med_pairs(med_period, target_lookahead_days, short_days)

        if not possible_med_pairs:
            return tuple([clearing_days + short_days] * 4)

        candidates = set()
        for med_period, med_num_periods in possible_med_pairs:
            possible_long_pairs = self._get_possible_long_pairs(
                med_period,
                med_num_periods,
                target_lookahead_days,
                short_days,
            )
            for long_period, long_num_periods in possible_long_pairs:
                med_days = med_period * med_num_periods
                long_days = long_period * long_num_periods
                candidate = (med_days, long_days, med_period, long_period)
                candidates.add(candidate)

        target_med_days = self.get_target_med_days()
        target_long_storage_days = self.get_target_long_storage_days()

        def distance_from_targets(candidate: tuple[int, int, int, int]) -> float | int:
            med_days = candidate[0]
            long_storage_days = candidate[3]
            med_square_diff = (target_med_days - med_days) ** 2
            long_square_diff = (target_long_storage_days - long_storage_days) ** 2
            return med_square_diff + long_square_diff

        return min(candidates, key=distance_from_targets)

    def _get_possible_med_pairs(
        self,
        med_period: int,
        target_lookahead_days: int,
        short_days: int,
    ) -> list[tuple[int, int]]:
        """Fuzz number of med storage periods to get more candidates."""
        target_med_days = self.get_target_med_days()
        out = []
        n = math.ceil(target_med_days / med_period)
        for m in [n - 1, n, n + 1]:
            implied_target_lookahead_days = med_period * (m + 1) + short_days
            if m > 1 and implied_target_lookahead_days <= target_lookahead_days:
                out.append((med_period, m))
        return out

    def _get_possible_long_pairs(
        self,
        med_period: int,
        med_num_periods: int,
        target_lookahead_days: int,
        short_days: int,
    ) -> list[tuple[int, int]]:
        """Find valid long pairs. Valid if long_period is divisor of med_days."""
        target_long_storage_days = self.get_target_long_storage_days()
        med_days = med_period * med_num_periods
        divisors = self._get_divisors(med_days)
        divisors = sorted(divisors, key=lambda x: abs(target_long_storage_days - x))
        divisors = divisors[:4]
        out = []
        for long_period in divisors:
            long_num_periods = math.ceil(max(1, target_lookahead_days - med_days - short_days) / long_period)
            out.append((long_period, long_num_periods))
        return out

    def _get_divisors(self, n: int) -> list[int]:
        """Return sorted list of divisors of n.

        Inspiration from: https://stackoverflow.com/questions/171765/what-is-the-best-way-to-get-all-the-divisors-of-a-number
        """
        divs = [1]
        for i in range(2, int(math.sqrt(n)) + 1):
            if n % i == 0:
                divs.extend([i, int(n / i)])
        divs.extend([n])
        return sorted(list(set(divs)))

    def _get_incremented_divisor(self, n: int, divisor: int, num_increments: int) -> int:
        """Get divisor of n num_increments greater than k if <= n else n."""
        divs = self._get_divisors(n)
        try:
            i = divs.index(divisor)
        except ValueError:
            message = f"{divisor} is not a divisor of {n}."
            raise ValueError(message) from None
        return divs[min(i + num_increments, len(divs) - 1)]

    def _check_hourly(self, x: int) -> None:
        if not (x / 60).is_integer():
            message = "Currently, JulES only support hourly resolutions."
            raise ValueError(message)
