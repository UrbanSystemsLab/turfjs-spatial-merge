const MongoClient = require('mongodb').MongoClient
const cluster = require('cluster')
const numCPUs = require('os').cpus().length
const prompt = require('prompt')
var config = require('./config.json')

var clipFeaturesProcess = require('./modules/clipFeatures.js')
var addTilesToLayerProcess = require('./modules/addTilesToLayer.js')
var spatialMergeProcess = require('./modules/spatialMerge.js')
var createTilesProcess = require('./modules/createTiles.js')

var innerLayerCollection = config.innerLayerCollection // 'buildings'
var outerLayerCollection = config.outerLayerCollection // 'lots'
var cityBoundaryCollection = config.cityBoundaryCollection // 'cityBoundary'
var squareGridCollection = config.squareGridCollection // 'squareGrid'
var triangleGridCollection = config.triangleGridCollection // 'triangleGrid'
var spatialMergeAttributes = config.spatialMergeAttributes // Properties to be imported from outerLayerCollection to innerLayerCollection

const mongoUrl = config.mongoUrl // 'mongodb://localhost:27017/nycdb'

var childProcessPid = [] // Keep track of Child Processes PIDs

function displayChoices(cb) {
	console.log(`
Spatial-Join

Enter the step number to execute:
1.	Create tile girds for batch processing (required for next steps)
2.	Clip inner & outer layer features to city boundary
3.	Link inner & outer layer features to tile grids
4.	Spatial Merge: Add outer layer properties to inner layer properties

	`)
	prompt.start()
	prompt.get(['script'], function(err, result) {
		if (err) {
			console.error('Error: Bad Choice')
			process.exit()
		}
		cb(result.script)
	})
}

if (cluster.isMaster) {
	var db = null
	MongoClient.connect(mongoUrl)
		.then(_db => {
			db = _db // Make it available globally
		})
		.catch(err => {
			console.error('Could not connect to DB ', err)
		})

	displayChoices(choice => {
		switch (choice) {
			case '1':
				createTilesProcess.init(db, cityBoundaryCollection) // Single-Thread
				break
			case '2':
				clipFeatures(db) // Multi-Thread
				break
			case '3':
				addTilesToLayers(db) // Multi-Thread
				break
			case '4':
				spatialMerge(db) // Multi-Thread
				break
			default:
				console.error('Error: Bad Choice')
				process.exit()
				break
		}
	})

	// Start workers
	for (let i = 0; i < numCPUs; i++) {
		var master = cluster.fork()
		childProcessPid.push(master.process.pid)
	}

	// Message listener for Master to each worker
	for (const id in cluster.workers) {
		cluster.workers[id].on('message', (msg) => { messageHandler(msg) })
	}

	// Track all working processes
	var activeWorkers = childProcessPid.slice(0)

	// Handle incoming messages from workers, keeps track of working child processes
	var messageHandler = function(msg) {
		if (msg.action === 'done') {
			for (var i = activeWorkers.length - 1; i >= 0; i--) {
				if (msg.pid === activeWorkers[i]) {
					activeWorkers.splice(i, 1) // Remove child process form completion-due array
					if (activeWorkers.length < 1) { completedProcess(msg.completed) } // If all child processes are completed
				}
			}
		}
	}
}

if (cluster.isWorker) {
	var dbWorker = null
	MongoClient.connect(mongoUrl)
		.then(_db => { dbWorker = _db }) // Make it available globally within Worker
		.catch(err => { console.error('Could not connect to DB ', err) })

	// Each worker hold following 3 data arrays for comparing and merging features
	var squareTiles = []
	var triangleTiles = []
	var boundaries = []

	// Receive messages from the master process.
	process.on('message', (msg) => {
		// If managing worker specific data or executing a process
		if (msg.data) {
			switch (msg.data) {
				// Allocate tiles to be processed.
				case 'squares':
					squareTiles.push(msg.tile)
					break
				case 'triangles':
					triangleTiles.push(msg.tile)
					break
					// Allocate tiles to be processed.
				case 'addBoundary':
					boundaries.push(msg.boundary)
					break
			}
		} else if (msg.action) {
			executeWorkerProcess(dbWorker, msg.action) // Otherwise it is a script execution
		}
	})
}

// Execute scripts for respective worker
function executeWorkerProcess(db, action) {
	switch (action) {
		case 'clipFeatures':
			clipFeaturesProcess.init(db, boundaries, innerLayerCollection)
				.then(() => {
					return clipFeaturesProcess.init(db, boundaries, outerLayerCollection)
				})
				.then(() => { process.send({ action: 'done', completed: 'clipFeatures', pid: process.pid }) })
				.catch(err => { console.error(err) })
			break

		case 'addTilesToLayers':
			addTilesToLayerProcess.init(db, innerLayerCollection, squareTiles, triangleTiles)
				.then(() => {
					return addTilesToLayerProcess.init(db, outerLayerCollection, squareTiles, triangleTiles)
				})
				.then(() => { process.send({ action: 'done', completed: 'addTilesToLayers', pid: process.pid }) })
				.catch(err => { console.error(err) })
			break

		case 'spatialMerge':
			spatialMergeProcess.init(db, innerLayerCollection, outerLayerCollection, squareTiles, triangleTiles, spatialMergeAttributes)
				.then(() => { process.send({ action: 'done', completed: 'spatialMerge', pid: process.pid }) })
				.catch(err => { console.error(err) })
			break
	}
}

// ----------------------------------------------------//

// Clipping features to city boundaries
function clipFeatures(db) {
	console.log('Allocating boundaries to workers')
	allocateBoundariesToWorkers(db)
		.then(() => { orderAllWorkers('clipFeatures') })
		.catch(err => { console.error(err) })
}

// Execute linking tiles to features on each child process
function addTilesToLayers(db) {
	console.log('Allocating tiles to workers')
	allocateTilesToWorkers(db, 'squares')
		.then(db => {
			return allocateTilesToWorkers(db, 'triangles')
		})
		.then(db => { orderAllWorkers('addTilesToLayers') })
		.catch(err => { console.error(err) })
}

// Execute spatial merger on each child process
function spatialMerge(db) {
	console.log('Allocating square tiles to workers')
	allocateTilesToWorkers(db, 'squares')
		.then(db => {
			console.log('Allocating triangle tiles to workers')
			return allocateTilesToWorkers(db, 'triangles')
		})
		.then(db => { orderAllWorkers('spatialMerge') })
		.catch(err => { console.error(err) })
}

// Sending action commands to child workers 
function orderAllWorkers(_action) {
	for (var id in cluster.workers) {
		cluster.workers[id].send({ action: _action })
	}
}

// Housekeeping work once all child processes are completed 
function completedProcess(processName) {
	switch (processName) {
		case 'clipFeatures':
			console.log('Clipped Inner and Outer Layers')
			db.collection(outerLayerCollection).drop()
				.then(() => {
					return db.collection(`${outerLayerCollection}_temp`).rename(outerLayerCollection)
				})
				.then(() => {
					return db.collection(innerLayerCollection).drop()
				})
				.then(() => {
					return db.collection(`${innerLayerCollection}_temp`).rename(innerLayerCollection)
				})
				.then(() => {
					console.log('Completed: Clipping Features')
					activeWorkers = childProcessPid.slice(0) // Reset Completion Status
					console.log('Press "ctrl+c" to exit')
						// process.exit()
				})
				.catch(err => { console.error(`Error: Could not rename collection ${outerLayerCollection}_temp to lots`, err) })
			break

		case 'addTilesToLayers':
			console.log('Completed: Adding tile_id to inner & outer features')
			activeWorkers = childProcessPid.slice(0)
			break

		case 'spatialMerge':
			console.log(`Completed: Spatial Merge between ${outerLayerCollection} & ${innerLayerCollection}`)
			activeWorkers = childProcessPid.slice(0)
			break
	}
}

// Distributing city boundary polygons to child processes for batch processing
function allocateBoundariesToWorkers(db) {
	return new Promise((resolve, reject) => {
		var workerNum = 0
		var boundaryCursor = db.collection(cityBoundaryCollection).find()

		// Wait for 3 sec before all workers are up and running
		setTimeout(function() {
			nextBoundary()
		}, 3000)

		function nextBoundary() {
			boundaryCursor.nextObject((err, boundary) => {
				if (!err && boundary !== null) {
					workerNum++
					cluster.workers[workerNum].send({ data: 'addBoundary', boundary: boundary })
					if (workerNum === numCPUs) {
						workerNum = 0
					}
					nextBoundary()
				} else resolve() // All boundaries have been allocated.
			})
		}
	})
}

// Distributing tiles to child processes for batch processing
function allocateTilesToWorkers(db, tileType) {
	var collection = null

	if (tileType === 'squares') collection = squareGridCollection
	else if (tileType === 'triangles') collection = triangleGridCollection

	return new Promise((resolve, reject) => {
		db.collection(collection).find().count()
			.then(count => {
				return count
			})
			.then(count => {
				// Cycle through tile collection and send tile_ID to each worker
				var workerNum = 0
				var tileCursor = db.collection(collection).find()

				// Wait for 3 sec before all workers are up and running
				setTimeout(() => { nextTile() }, 3000)

				function nextTile() {
					tileCursor.nextObject((err, tile) => {
						if (!err && tile !== null) {
							workerNum++
							cluster.workers[workerNum].send({ data: tileType, tile: tile })
							if (workerNum === numCPUs) { workerNum = 0 }
							nextTile()
						} else resolve(db) // All tiles have been allocated.
					})
				}
			})
			.catch(err => { reject(err) })
	})
}

cluster.on('exit', (worker, code, signal) => {
	console.log(`worker ${worker.process.pid} died`)
})
