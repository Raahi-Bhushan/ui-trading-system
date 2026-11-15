"""Tests for the WebSocket watchdog mechanism in common_lib.py.

Tests cover:
- update_last_tick_time() updates the global timestamp
- watchdog_sleep() detects stale ticks and triggers reconnection
- watchdog_sleep() resets last_tick_time after successful reconnect
- watchdog_sleep() enforces max reconnect attempts
- _on_noreconnect_handler() logs the exhaustion event
- initialise_ticker() stores callbacks for reconnection
- initialise_ticker() uses reactor.callFromThread for reconnections
- subscribe_for_tick() stores subscribed tokens
"""

import time
from unittest.mock import patch, MagicMock, call
import pytest


@pytest.fixture(autouse=True)
def reset_globals():
    """Reset common_lib globals before each test."""
    import common_lib
    common_lib.last_tick_time = 0
    common_lib._stored_ticker_callbacks = {}
    common_lib._subscribed_tokens = []
    common_lib.kws = None
    yield
    common_lib.last_tick_time = 0
    common_lib._stored_ticker_callbacks = {}
    common_lib._subscribed_tokens = []
    common_lib.kws = None


class TestUpdateLastTickTime:
    """Tests for update_last_tick_time function."""

    def test_updates_timestamp(self):
        """Test that update_last_tick_time sets last_tick_time to current time."""
        # Arrange
        import common_lib
        assert common_lib.last_tick_time == 0

        # Act
        before = time.time()
        common_lib.update_last_tick_time()
        after = time.time()

        # Assert
        assert before <= common_lib.last_tick_time <= after

    def test_updates_on_successive_calls(self):
        """Test that successive calls update the timestamp."""
        # Arrange
        import common_lib

        # Act
        common_lib.update_last_tick_time()
        first_time = common_lib.last_tick_time
        time.sleep(0.01)
        common_lib.update_last_tick_time()
        second_time = common_lib.last_tick_time

        # Assert
        assert second_time > first_time


class TestOnNoreconnectHandler:
    """Tests for _on_noreconnect_handler callback."""

    def test_logs_error_on_noreconnect(self):
        """Test that handler logs error when reconnection exhausted."""
        # Arrange
        import common_lib

        # Act & Assert
        with patch('common_lib.logging') as mock_logging:
            common_lib._on_noreconnect_handler(MagicMock())
            mock_logging.error.assert_called_once()
            assert "exhausted" in mock_logging.error.call_args[0][0].lower()


class TestSubscribeForTick:
    """Tests for subscribe_for_tick storing tokens."""

    def test_stores_tokens(self):
        """Test that subscribe_for_tick stores tokens in _subscribed_tokens."""
        # Arrange
        import common_lib
        mock_kws = MagicMock()
        common_lib.kws = mock_kws
        tokens = [256265, 260105]

        # Act
        common_lib.subscribe_for_tick(tokens)

        # Assert
        assert common_lib._subscribed_tokens == [256265, 260105]
        mock_kws.subscribe.assert_called_once_with(tokens)
        mock_kws.set_mode.assert_called_once_with(mock_kws.MODE_FULL, tokens)

    def test_overwrites_previous_tokens(self):
        """Test that a new call replaces previously stored tokens."""
        # Arrange
        import common_lib
        mock_kws = MagicMock()
        common_lib.kws = mock_kws
        common_lib._subscribed_tokens = [111, 222]

        # Act
        common_lib.subscribe_for_tick([333, 444])

        # Assert
        assert common_lib._subscribed_tokens == [333, 444]


class TestInitialiseTicker:
    """Tests for initialise_ticker (Upstox KiteTicker: own thread, no Twisted reactor)."""

    @pytest.fixture(autouse=True)
    def _reset(self):
        import common_lib
        common_lib.kws = None
        common_lib._subscribed_tokens = []
        common_lib.access_token = "test_token"
        yield
        common_lib.kws = None

    @patch('common_lib.KiteTicker')
    def test_stores_callbacks(self, mock_ticker_class):
        """Test that initialise_ticker stores callbacks for watchdog reuse."""
        import common_lib
        mock_ticker_class.return_value = MagicMock()
        on_ticks, on_connect, on_order = MagicMock(), MagicMock(), MagicMock()

        common_lib.initialise_ticker(on_ticks, on_connect, on_order)

        assert common_lib._stored_ticker_callbacks == {
            'on_ticks': on_ticks,
            'on_connect': on_connect,
            'on_order_update': on_order
        }

    @patch('common_lib.KiteTicker')
    def test_sets_noreconnect_handler(self, mock_ticker_class):
        """Test that initialise_ticker sets on_noreconnect callback."""
        import common_lib
        mock_kws = MagicMock()
        mock_ticker_class.return_value = mock_kws

        common_lib.initialise_ticker(MagicMock(), MagicMock(), MagicMock())

        assert mock_kws.on_noreconnect == common_lib._on_noreconnect_handler

    @patch('common_lib.KiteTicker')
    def test_first_connect_starts_thread(self, mock_ticker_class):
        """First call starts the ticker in its own thread."""
        import common_lib
        mock_kws = MagicMock()
        mock_ticker_class.return_value = mock_kws

        common_lib.initialise_ticker(MagicMock(), MagicMock(), MagicMock())

        mock_kws.connect.assert_called_once_with(threaded=True)

    @patch('common_lib.KiteTicker')
    def test_reconnect_closes_previous_ticker(self, mock_ticker_class):
        """A watchdog re-init closes the old ticker before starting a new one."""
        import common_lib
        old = MagicMock()
        common_lib.kws = old
        common_lib._subscribed_tokens = [256265]
        new = MagicMock()
        mock_ticker_class.return_value = new

        common_lib.initialise_ticker(MagicMock(), MagicMock(), MagicMock())

        old.close.assert_called_once()
        new.connect.assert_called_once_with(threaded=True)
        assert common_lib.kws is new

    @patch('common_lib.KiteTicker')
    def test_reconnect_wraps_on_connect_for_resubscribe(self, mock_ticker_class):
        """Test that reconnect wraps on_connect to auto-resubscribe tokens."""
        import common_lib
        common_lib.kws = MagicMock()
        common_lib._subscribed_tokens = [256265, 260105]
        mock_kws = MagicMock()
        mock_ticker_class.return_value = mock_kws
        original_on_connect = MagicMock()

        common_lib.initialise_ticker(MagicMock(), original_on_connect, MagicMock())
        with patch.object(common_lib, 'subscribe_for_tick') as mock_sub:
            mock_kws.on_connect(MagicMock(), MagicMock())

        original_on_connect.assert_called_once()
        mock_sub.assert_called_once_with([256265, 260105])


class TestWatchdogSleep:
    """Tests for watchdog_sleep function."""

    @patch('common_lib.time.sleep')
    def test_completes_normally_when_ticks_flow(self, mock_sleep):
        """Test watchdog completes without reconnecting when ticks are fresh."""
        # Arrange
        import common_lib
        common_lib.last_tick_time = time.time()
        # Make time.sleep a no-op but track calls
        mock_sleep.return_value = None

        # Patch time.time to return incrementing values
        call_count = [0]
        base_time = time.time()

        def fake_time():
            call_count[0] += 1
            # Each call returns ~1 second later but within stale threshold
            return base_time + call_count[0] * 0.1

        # Act - run with very short total_seconds so it finishes fast
        with patch('common_lib.time.time', side_effect=fake_time):
            common_lib.watchdog_sleep(
                total_seconds=30, check_interval=30, stale_threshold=60
            )

        # Assert - sleep was called once (for the one 30s interval)
        assert mock_sleep.call_count == 1

    @patch('common_lib.initialise_ticker')
    @patch('common_lib.time.sleep')
    def test_reconnects_when_ticks_stale(self, mock_sleep, mock_init_ticker):
        """Test watchdog triggers reconnection when ticks are stale."""
        # Arrange
        import common_lib
        # Set last_tick_time to 120 seconds ago (beyond 60s threshold)
        common_lib.last_tick_time = time.time() - 120
        common_lib._stored_ticker_callbacks = {
            'on_ticks': MagicMock(),
            'on_connect': MagicMock(),
            'on_order_update': MagicMock()
        }
        mock_kws = MagicMock()
        common_lib.kws = mock_kws

        # Act - one check interval, then done
        # Use a short total_seconds so it runs once
        # Patch time.time to ensure it's always advanced beyond threshold
        current_time = time.time()
        with patch('common_lib.time.time', side_effect=[current_time + 150, current_time + 151, current_time + 152]):
            common_lib.watchdog_sleep(
                total_seconds=30, check_interval=30, stale_threshold=60
            )

        # Assert - should have called initialise_ticker to reconnect
        mock_init_ticker.assert_called_once()
        # Should have tried to close old connection
        mock_kws.close.assert_called_once()

    @patch('common_lib.initialise_ticker')
    @patch('common_lib.time.sleep')
    def test_resets_last_tick_time_after_reconnect(self, mock_sleep,
                                                    mock_init_ticker):
        """Test watchdog resets last_tick_time to 0 after successful reconnect.

        This prevents the stale-tick counter from growing indefinitely."""
        # Arrange
        import common_lib
        common_lib.last_tick_time = time.time() - 120
        common_lib._stored_ticker_callbacks = {
            'on_ticks': MagicMock(),
            'on_connect': MagicMock(),
            'on_order_update': MagicMock()
        }
        common_lib.kws = MagicMock()

        # Act
        current_time = time.time()
        with patch('common_lib.time.time', side_effect=[current_time + 150, current_time + 151, current_time + 152]):
            common_lib.watchdog_sleep(
                total_seconds=30, check_interval=30, stale_threshold=60
            )

        # Assert - last_tick_time should be reset to 0
        assert common_lib.last_tick_time == 0

    @patch('common_lib.initialise_ticker')
    @patch('common_lib.time.sleep')
    def test_max_reconnect_attempts_enforced(self, mock_sleep,
                                              mock_init_ticker):
        """Test watchdog stops after max_reconnect_attempts exceeded."""
        # Arrange
        import common_lib
        # Simulate ticks being stale forever (init_ticker doesn't fix it
        # because we don't actually reset in the mock)
        original_time = time.time()
        common_lib.last_tick_time = original_time - 120
        common_lib._stored_ticker_callbacks = {
            'on_ticks': MagicMock(),
            'on_connect': MagicMock(),
            'on_order_update': MagicMock()
        }
        common_lib.kws = MagicMock()

        # Make initialise_ticker NOT reset last_tick_time (simulating failure)
        # Override the actual reset by setting it back after each sleep
        real_sleep_calls = [0]
        old_ltt = original_time - 120

        def side_effect_sleep(seconds):
            """After the watchdog resets last_tick_time=0, force it back
            to stale to simulate continued failure."""
            real_sleep_calls[0] += 1
            if common_lib.last_tick_time == 0:
                common_lib.last_tick_time = old_ltt

        mock_sleep.side_effect = side_effect_sleep

        # Act - use enough total_seconds for many iterations
        with patch('common_lib.logging'):
            # Provide enough time values for multiple iterations
            curr = time.time()
            time_values = [curr + 150 + (i*30) for i in range(10)]
            with patch('common_lib.time.time', side_effect=time_values):
                common_lib.watchdog_sleep(
                    total_seconds=6000, check_interval=30,
                    stale_threshold=60, max_reconnect_attempts=3
                )

        # Assert - should have attempted reconnect exactly 3 times
        # (the 4th iteration breaks out of the loop)
        assert mock_init_ticker.call_count == 3

    @patch('common_lib.time.sleep')
    def test_skips_check_when_no_ticks_yet(self, mock_sleep):
        """Test watchdog skips stale check if no ticks received yet."""
        # Arrange
        import common_lib
        common_lib.last_tick_time = 0  # No ticks received

        # Act
        with patch('common_lib.logging') as mock_logging:
            common_lib.watchdog_sleep(
                total_seconds=30, check_interval=30, stale_threshold=60
            )

        # Assert - final log should be completion, no reconnection attempt
        error_calls = [
            str(c) for c in mock_logging.error.call_args_list
        ]
        reconnect_calls = [c for c in error_calls if 'Re-initializing' in c]
        assert len(reconnect_calls) == 0

    @patch('common_lib.initialise_ticker')
    @patch('common_lib.time.sleep')
    def test_handles_close_error_gracefully(self, mock_sleep, mock_init_ticker):
        """Test watchdog handles error when closing old ticker."""
        # Arrange
        import common_lib
        common_lib.last_tick_time = time.time() - 120
        common_lib._stored_ticker_callbacks = {
            'on_ticks': MagicMock(),
            'on_connect': MagicMock(),
            'on_order_update': MagicMock()
        }
        mock_kws = MagicMock()
        mock_kws.close.side_effect = Exception("Already closed")
        common_lib.kws = mock_kws

        # Act - should not raise
        current_time = time.time()
        with patch('common_lib.time.time', side_effect=[current_time + 150, current_time + 151, current_time + 152]):
            common_lib.watchdog_sleep(
                total_seconds=30, check_interval=30, stale_threshold=60
            )

        # Assert - still attempted to reconnect despite close error
        mock_init_ticker.assert_called_once()

    @patch('common_lib.time.sleep')
    def test_logs_error_when_no_stored_callbacks(self, mock_sleep):
        """Test watchdog logs error when callbacks not stored."""
        # Arrange
        import common_lib
        common_lib.last_tick_time = time.time() - 120
        common_lib._stored_ticker_callbacks = {}

        # Act
        with patch('common_lib.logging') as mock_logging:
            common_lib.watchdog_sleep(
                total_seconds=30, check_interval=30, stale_threshold=60
            )

        # Assert
        error_calls = [
            str(c) for c in mock_logging.error.call_args_list
        ]
        no_callback_calls = [
            c for c in error_calls if 'No stored callbacks' in c
        ]
        assert len(no_callback_calls) == 1

    @patch('common_lib.initialise_ticker')
    @patch('common_lib.time.sleep')
    def test_resets_consecutive_reconnects_on_fresh_tick(self, mock_sleep,
                                                         mock_init_ticker):
        """Test that consecutive_reconnects resets when ticks resume."""
        # Arrange
        import common_lib
        common_lib.last_tick_time = time.time() - 120
        common_lib._stored_ticker_callbacks = {
            'on_ticks': MagicMock(),
            'on_connect': MagicMock(),
            'on_order_update': MagicMock()
        }
        common_lib.kws = MagicMock()

        iteration = [0]

        def side_effect_sleep(seconds):
            """Simulate: first reconnect attempt, then ticks resume."""
            iteration[0] += 1
            # After the first reconnect+wait, simulate ticks resuming
            if iteration[0] >= 4:
                common_lib.last_tick_time = time.time()

        mock_sleep.side_effect = side_effect_sleep

        # Act
        # Provide time values: first few are stale, then one is fresh
        curr = time.time()
        # iteration 1: curr+150 (stale)
        # iteration 2: curr+180 (stale)
        # iteration 3: curr+210 (stale)
        # iteration 4: curr+500 (but we reset last_tick_time to now in side_effect_sleep)
        time_values = [curr + 150, curr + 180, curr + 210, curr + 500, curr + 510, curr + 520]
        with patch('common_lib.time.time', side_effect=time_values):
            common_lib.watchdog_sleep(
                total_seconds=90, check_interval=30, stale_threshold=60,
                max_reconnect_attempts=3
            )

        # Assert - at least one reconnect happened but didn't hit max
        assert mock_init_ticker.call_count >= 1
        assert mock_init_ticker.call_count <= 3


class TestWriteSpotPrice:
    """Tests for write_spot_price function."""

    def test_writes_nifty_price(self, tmp_path):
        """Test writing NIFTY price creates the spot file."""
        # Arrange
        import common_lib
        status_dir = tmp_path / "survivor_status"
        status_dir.mkdir(parents=True, exist_ok=True)
        spot_file = status_dir / "spot_prices.json"

        # Act
        with patch('common_lib.os.path.join', return_value=str(spot_file)):
            common_lib.write_spot_price("NIFTY", 25900.5)

        # Assert
        import json
        assert spot_file.exists()
        with open(spot_file) as f:
            data = json.load(f)
        assert data["nifty_price"] == 25900.5
        assert "nifty_updated" in data

    def test_preserves_existing_data(self, tmp_path):
        """Test writing one index preserves the other's data."""
        # Arrange
        import common_lib
        import json
        status_dir = tmp_path / "survivor_status"
        status_dir.mkdir(parents=True, exist_ok=True)
        spot_file = status_dir / "spot_prices.json"
        
        with open(spot_file, 'w') as f:
            json.dump({"nifty_price": 25900.0,
                       "nifty_updated": "2026-01-01"}, f)

        # Act
        with patch('common_lib.os.path.join', return_value=str(spot_file)):
            common_lib.write_spot_price("SENSEX", 85000.0)

        # Assert
        with open(spot_file) as f:
            data = json.load(f)
        assert data["nifty_price"] == 25900.0
        assert data["sensex_price"] == 85000.0

    def test_handles_write_error_gracefully(self):
        """Test that write errors are caught and logged."""
        # Arrange
        import common_lib

        # Act & Assert - should not raise
        # Mock open to raise an error
        with patch("builtins.open", side_effect=IOError("Permission denied")):
            with patch('common_lib.logging') as mock_log:
                common_lib.write_spot_price("NIFTY", 25900.0)
                mock_log.warning.assert_called_once()
