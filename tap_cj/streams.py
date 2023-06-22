"""Stream type classes for tap-cj."""

from __future__ import annotations

from tap_cj.client import CJStream


class CommissionsStream(CJStream):
    """Define custom stream."""

    name = "commissions"
    # Optionally, you may also use `schema_filepath` in place of `schema`:
    schema_filepath = "schemas/commissions.schema.json"

    @property
    def partitions(self) -> list[dict] | None:
        return [
            {"publisher_id": publisher_id}
            for publisher_id in self.config["publisher_ids"]
        ]

    @property
    def next_page_token(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config.get("start_date", "")

    @property
    def query(self) -> str:
        return """
        publisherCommissions(
            forPublishers: ["$PUB_ID"],
            sincePostingDate:"$FROM_DATET00:00:00Z",
            beforePostingDate:"$TO_DATET00:00:00Z"
        ){
            count
            payloadComplete
            records
            {actionTrackerName
            actionTrackerId
            websiteName
            advertiserName
            postingDate
            eventDate
            commissionId
            clickDate
            actionStatus
            actionType
            shopperId
            publisherId
            websiteId
            advertiserId
            orderDiscountUsd
            clickReferringURL
            pubCommissionAmountUsd
            saleAmountUsd
            orderId
            source
            items
            {
                quantity
            perItemSaleAmountPubCurrency
            totalCommissionPubCurrency
            sku
            }
            verticalAttributes
            {
                itemName
            brand
            }

            }

            }
        """
