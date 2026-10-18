import pytest
from src.application.use_cases import ProduceEventsUseCase
from src.domain.generators import EventGenerator


class MockPublisher:
    def __init__(self):
        self.messages = []
        self.dlq_messages = []

    def publish(self, topic, key, message):
        self.messages.append((topic, key, message))

    def publish_dlq(self, dlq_topic, key, error_payload):
        self.dlq_messages.append((dlq_topic, key, error_payload))


def test_event_generator():
    event = EventGenerator.create_random_event()
    assert event.event_id is not None
    assert event.event_type is not None


def test_use_case(mocker):
    # Mock sleep to avoid waiting
    mocker.patch("time.sleep", return_value=None)

    mock_publisher = MockPublisher()
    use_case = ProduceEventsUseCase(mock_publisher, "test-topic", 0.1, "events-dlq")

    # We use KeyboardInterrupt so it is not caught by 'except Exception:'
    mocker.patch(
        "src.domain.generators.EventGenerator.create_random_event",
        side_effect=[EventGenerator.create_random_event(), KeyboardInterrupt("Break")],
    )

    with pytest.raises(KeyboardInterrupt, match="Break"):
        use_case.execute()

    assert len(mock_publisher.messages) == 1
    topic, key, msg = mock_publisher.messages[0]
    assert topic == "test-topic"
    assert "event_id" in msg


def test_use_case_dlq_on_error(mocker):
    mocker.patch("time.sleep", return_value=None)

    mock_publisher = MockPublisher()
    mocker.patch.object(
        mock_publisher,
        "publish",
        side_effect=Exception("Kafka failure"),
    )

    mocker.patch(
        "src.domain.generators.EventGenerator.create_random_event",
        side_effect=[EventGenerator.create_random_event(), KeyboardInterrupt("Break")],
    )

    use_case = ProduceEventsUseCase(mock_publisher, "test-topic", 0.1, "events-dlq")

    with pytest.raises(KeyboardInterrupt, match="Break"):
        use_case.execute()

    assert len(mock_publisher.dlq_messages) == 1
    dlq_topic, key, dlq_msg = mock_publisher.dlq_messages[0]
    assert dlq_topic == "events-dlq"
    assert "Kafka failure" in dlq_msg["error_reason"]
