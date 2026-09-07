import { deliveryAcknowledgement, deliveryExpectation } from "../../packages/collector-cli/src/delivery-ack";

/** Test receiver success after its fixture has accepted/stored the request. */
export function acceptedFixtureDelivery(rawBody: string, installKey: string) {
  const expected = deliveryExpectation(rawBody, installKey);
  return { ok: true, accepted: expected.itemIds.length,
    ack: deliveryAcknowledgement(expected, expected.itemIds) };
}
