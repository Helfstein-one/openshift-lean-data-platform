import pytest
from src.application.use_cases import ProduceEventsUseCase
from src.domain.generators import EventGenerator


class MockPublisher:
    def __init__(self):
        self.messages = []

    def publish(self, topic, key, message):
        self.messages.append((topic, key, message))


def test_event_generator():
    event = EventGenerator.create_random_event()
    assert event.event_id is not None
    assert event.event_type is not None


def test_use_case(mocker):
    # Mock sleep to avoid waiting
    mocker.patch("time.sleep", return_value=None)

    mock_publisher = MockPublisher()
    use_case = ProduceEventsUseCase(mock_publisher, "test-topic", 0.1)

    # We use a trick to break the infinite loop after 1 iteration
    mocker.patch(
        "src.domain.generators.EventGenerator.create_random_event",
        side_effect=[EventGenerator.create_random_event(), Exception("Break")],
    )

    with pytest.raises(Exception, match="Break"):
        use_case.execute()

    assert len(mock_publisher.messages) == 1
    topic, key, msg = mock_publisher.messages[0]
    assert topic == "test-topic"
    assert "event_id" in msg
