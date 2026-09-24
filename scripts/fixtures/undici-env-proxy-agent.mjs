// Node 22.20 and older do not implement NODE_USE_ENV_PROXY for fetch.
// Install the same environment proxy policy in proof children on those nodes.
import { EnvHttpProxyAgent, setGlobalDispatcher } from "undici";

setGlobalDispatcher(new EnvHttpProxyAgent());
