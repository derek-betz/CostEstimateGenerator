import pandas as pd


def make_summary_text(items_df: pd.DataFrame) -> str:
    total = float((items_df["QUANTITY"] * items_df["UNIT_PRICE_EST"]).sum())
    top = items_df.sort_values("QUANTITY", ascending=False).head(5)[
        ["ITEM_CODE", "DESCRIPTION", "QUANTITY", "UNIT_PRICE_EST"]
    ]
    return (
        f"Project subtotal (items x unit price): ${total:,.0f}.\n"
        f"Top quantity drivers:\n{top.to_string(index=False)}\n"
        "Pricing used BidTabs data with configured hierarchy (e.g., District + State) and time window.\n"
    )
