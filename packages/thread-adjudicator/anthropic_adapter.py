"""Only imported on an explicit --provider anthropic run. Never logs secrets."""
import os


class AnthropicAdapter:
    def __init__(self):
        self.client = None

    def __call__(self, params, _items, _baseline):
        if self.client is None:
            import anthropic
            # The conductor supplies its existing key in process. No vault,
            # credential discovery, fallback accounts, or purchase path here.
            self.client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"], max_retries=0, timeout=900)
        with self.client.messages.stream(**params) as stream:
            msg = stream.get_final_message()
        import json
        usage = None if msg.usage is None else {k: getattr(msg.usage, k, 0) or 0 for k in
                                               ("input_tokens", "cache_read_input_tokens", "cache_creation_input_tokens", "output_tokens")}
        try:
            response = json.loads("".join(b.text for b in msg.content if b.type == "text")) if msg.stop_reason in ("end_turn", "stop_sequence") else {"_error": "incomplete"}
        except (ValueError, TypeError):
            response = {"_error": "invalid_json"}
        return response, usage, msg.model
