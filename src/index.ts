import { readFileSync } from "fs";
import * as http from "http";
import * as https from "https";
import * as net from "net";
import { join } from "path";
import * as tls from "tls";
import { randomUUID } from "crypto";
import Account from "./account";
import routes from "./routes";
import { createStorageFactory, loadStorageConfig } from "./storage/storage-factory";
import { setDebugEnabled } from "./debug";

const generateRequestHandler = ({
  keepAlive = false
}: {
  /**
   * If set to `true` adds `Connection: keep-alive` header, otherwise uses
   * `Connection: close`.
   */
  keepAlive?: boolean | undefined;
}) => (
  account: Account,
  req: http.IncomingMessage,
  res: http.ServerResponse
) => {
  const route = routes(req);

  (async () => {
    let body;
    if (route) {
      const [params, handler] = route;
      try {
        body = await handler(account, req, res, params);
      } catch (err) {
        // eslint-disable-next-line no-console
        console.error(err);
        body = { message: err.message };
        res.statusCode = 500;
      }
      if (res.statusCode > 399 && !body.message) {
        body.message = "";
      }
    } else {
      res.statusCode = 400;
      body = { message: "no route" };
    }

    if (body && body._etag) {
      res.setHeader("etag", body._etag);
    }

    res.setHeader("content-type", "application/json");
    res.setHeader("content-location", `https://${req.headers.host}${req.url}`);
    res.setHeader("connection", keepAlive ? "keep-alive" : "close");
    res.setHeader("x-ms-activity-id", randomUUID());
    res.setHeader("x-ms-request-charge", "1");
    if (req.headers["x-ms-documentdb-populatequerymetrics"]) {
      res.setHeader(
        "x-ms-documentdb-query-metrics",
        "totalExecutionTimeInMs=0.00;queryCompileTimeInMs=0.00;queryLogicalPlanBuildTimeInMs=0.00;queryPhysicalPlanBuildTimeInMs=0.00;queryOptimizationTimeInMs=0.00;VMExecutionTimeInMs=0.00;indexLookupTimeInMs=0.00;documentLoadTimeInMs=0.00;systemFunctionExecuteTimeInMs=0.00;userFunctionExecuteTimeInMs=0.00;retrievedDocumentCount=0;retrievedDocumentSize=0;outputDocumentCount=1;outputDocumentSize=0;writeOutputTimeInMs=0.00;indexUtilizationRatio=0.00"
      );
    }

    if (req.headers["a-im"] && res.statusCode === 200) {
      if (req.headers["if-none-match"]) {
        res.statusCode = 304;
      }
      res.setHeader("etag", "1");
    }
    res.end(JSON.stringify(body));
  })().catch(err => {
    // eslint-disable-next-line no-console
    console.error(err);
    if (!res.finished) {
      res.statusCode = 500;
      res.end("");
    }
  });
};

const createAccount = async (address: string | net.AddressInfo | null, storageMode?: string, debug?: boolean): Promise<Account> => {
  if (!address || typeof address !== "object") {
    throw new Error(`Unexpected address type: ${address}`);
  }

  // Set global debug state
  if (debug) {
    setDebugEnabled(true);
  }

  const { address: host, port } = address as net.AddressInfo;
  const hostname = host === "0.0.0.0" || host === "::" ? "localhost" : host;

  // Create storage factory if storage mode is specified
  let account: Account;
  if (storageMode && storageMode !== 'memory') {
    const storageConfig = loadStorageConfig(storageMode);
    const storageFactory = createStorageFactory(storageConfig);
    account = new Account(hostname, port, storageFactory);
  } else {
    account = new Account(hostname, port);
  }

  // Initialize account to restore data from persistent storage
  await account.initialize();
  
  return account;
};

export function createHttpServer(opts: http.ServerOptions & { storageMode?: string; debug?: boolean } = {}) {
  let account: Account | undefined;

  const handleRequest = generateRequestHandler({
    keepAlive: opts.keepAlive
  });
  const server = http
    .createServer(opts, (req, res) => {
      if (!account) {
        res.statusCode = 503;
        res.setHeader('content-type', 'application/json');
        res.end(JSON.stringify({ message: 'Server still initializing' }));
        return;
      }
      handleRequest(account, req, res);
    })
    .on("listening", async () => {
      try {
        account = await createAccount(server.address(), opts.storageMode, opts.debug);
        console.log('Server ready and account initialized');
      } catch (error) {
        console.error('Failed to initialize account:', error);
        server.close();
      }
    });

  return server;
}

const getCertificate = (name: string) => {
  return readFileSync(join(__dirname, "..", name));
};

export function createHttpsServer(opts: https.ServerOptions & { storageMode?: string; debug?: boolean } = {}) {
  let account: Account | undefined;

  const handleRequest = generateRequestHandler({
    keepAlive: opts.keepAlive
  });

  const options = {
    key: getCertificate("key.pem"),
    cert: getCertificate("cert.pem"),
    ...opts,
  };

  const server = https
    .createServer(options, (req, res) => {
      if (!account) {
        res.statusCode = 503;
        res.setHeader('content-type', 'application/json');
        res.end(JSON.stringify({ message: 'Server still initializing' }));
        return;
      }
      handleRequest(account, req, res);
    })
    .on("listening", async () => {
      try {
        account = await createAccount(server.address(), opts.storageMode, opts.debug);
        console.log('Server ready and account initialized');
      } catch (error) {
        console.error('Failed to initialize account:', error);
        server.close();
      }
    });

  return server;
}

export default createHttpsServer;
