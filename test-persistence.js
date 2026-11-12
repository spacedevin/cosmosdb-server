#!/usr/bin/env node

/**
 * Test script to verify database/collection persistence after restart
 */

const https = require('https');
const fs = require('fs');

// Disable TLS verification for self-signed cert
process.env["NODE_TLS_REJECT_UNAUTHORIZED"] = 0;

const baseUrl = 'https://localhost:8081';

async function makeRequest(method, path, data) {
  return new Promise((resolve, reject) => {
    const url = new URL(path, baseUrl);
    const options = {
      method,
      hostname: url.hostname,
      port: url.port,
      path: url.pathname + url.search,
      headers: {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer test-key'
      },
      rejectUnauthorized: false
    };

    if (data) {
      const jsonData = JSON.stringify(data);
      options.headers['Content-Length'] = Buffer.byteLength(jsonData);
    }

    const req = https.request(options, (res) => {
      let body = '';
      res.on('data', chunk => body += chunk);
      res.on('end', () => {
        try {
          const result = JSON.parse(body);
          resolve({ status: res.statusCode, data: result });
        } catch {
          resolve({ status: res.statusCode, data: body });
        }
      });
    });

    req.on('error', reject);

    if (data) {
      req.write(JSON.stringify(data));
    }
    req.end();
  });
}

async function testPersistence() {
  console.log('🔍 Testing database/collection persistence after restart...');

  try {
    // 1. Try to query the database that was created before restart
    console.log('\n1. Checking if test-db exists...');
    const dbResult = await makeRequest('GET', '/dbs/test-db');
    console.log(`Database check: ${dbResult.status}`, dbResult.data);

    // 2. Try to query the collection that was created before restart
    console.log('\n2. Checking if test-collection exists...');
    const collResult = await makeRequest('GET', '/dbs/test-db/colls/test-collection');
    console.log(`Collection check: ${collResult.status}`, collResult.data);

    // 3. Try to query documents (this should work if file storage is working)
    console.log('\n3. Trying to query documents...');
    const queryResult = await makeRequest('POST', '/dbs/test-db/colls/test-collection/docs', {
      query: 'SELECT * FROM c',
      parameters: []
    });
    console.log(`Document query: ${queryResult.status}`, queryResult.data);

    // 4. List all databases
    console.log('\n4. Listing all databases...');
    const dbsResult = await makeRequest('GET', '/dbs');
    console.log(`Databases list: ${dbsResult.status}`, dbsResult.data);

    // 5. Check storage directories
    console.log('\n5. Checking storage directories...');
    try {
      const dataDir = './cosmos-local-data';
      if (fs.existsSync(dataDir)) {
        console.log('✅ Data directory exists');
        
        if (fs.existsSync(dataDir + '/collections')) {
          const collections = fs.readdirSync(dataDir + '/collections', { withFileTypes: true });
          console.log(`✅ Found ${collections.length} collection directories`);
          
          collections.forEach(collection => {
            if (collection.isDirectory()) {
              const collPath = `${dataDir}/collections/${collection.name}`;
              console.log(`  Collection directory: ${collection.name}`);
              
              // Check for segments
              if (fs.existsSync(collPath + '/segments')) {
                const segmentFiles = fs.readdirSync(collPath + '/segments');
                console.log(`    📁 Segment files: ${segmentFiles.join(', ')}`);
                
                // Check segment file sizes
                segmentFiles.forEach(file => {
                  if (file.endsWith('.seg')) {
                    const stats = fs.statSync(`${collPath}/segments/${file}`);
                    console.log(`      ${file}: ${stats.size} bytes`);
                  }
                });
              }
            }
          });
        }
      } else {
        console.log('❌ Data directory not found');
      }
    } catch (error) {
      console.log('⚠️ Error checking directories:', error.message);
    }

  } catch (error) {
    console.error('❌ Test failed:', error);
  }
}

// Run the test
testPersistence().then(() => {
  console.log('\n🎯 Persistence test complete');
  process.exit(0);
}).catch(error => {
  console.error('💥 Test error:', error);
  process.exit(1);
});