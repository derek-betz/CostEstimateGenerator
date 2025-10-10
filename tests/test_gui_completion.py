import textwrap

from src.costest.gui import EstimatorApp


def _make_app() -> EstimatorApp:
    return EstimatorApp.__new__(EstimatorApp)  # type: ignore[arg-type]


def test_parse_completion_message_extracts_table_and_footer() -> None:
    message = textwrap.dedent(
        """
        Project subtotal (items x unit price): $211.
        Top Cost Drivers (Top 5):
        ITEM_CODE DESCRIPTION  QUANTITY  UNIT_PRICE_EST  TOTAL_COST
              002 Long desc 2        20             7.8       156.0
              001      Desc 1        10             5.5        55.0
        Pricing used BidTabs data with configured hierarchy (e.g., District + State) and time window.
        """
    ).strip()

    parsed = EstimatorApp._parse_completion_message(_make_app(), message)
    assert parsed is not None
    assert parsed["summary"] == ["Project subtotal (items x unit price): $211."]
    assert parsed["table_headers"] == [
        "Item Code",
        "Description",
        "Quantity",
        "Unit Price Est",
        "Total Cost",
    ]
    assert parsed["table_rows"] == [
        ["002", "Long desc 2", "20.0", "$7.80", "$156.00"],
        ["001", "Desc 1", "10.0", "$5.50", "$55.00"],
    ]
    assert parsed["footer"] == [
        "Pricing used BidTabs data with configured hierarchy (e.g., District + State) and time window."
    ]


def test_parse_completion_message_handles_nonstandard_heading_case() -> None:
    message = textwrap.dedent(
        """
        Project subtotal (items x unit price): $211.
        TOP COST DRIVER SUMMARY:
        ITEM_CODE DESCRIPTION  QUANTITY  UNIT_PRICE_EST  TOTAL_COST
              002 Long desc 2        20             7.8       156.0
        Pricing used BidTabs data with configured hierarchy (e.g., District + State) and time window.
        """
    ).strip()

    parsed = EstimatorApp._parse_completion_message(_make_app(), message)
    assert parsed is not None
    assert parsed["table_rows"][0][0] == "002"
