var turf = require('turf')

var collectionCursor
var squareTiles = []
var triangleTiles = []
var db, collection

var AddTilesToLayer = function() {
	this.init = function(_db, _collection, _squareTiles, _triangleTiles) {
		squareTiles = _squareTiles
		triangleTiles = _triangleTiles
		db = _db
		collection = _collection
		collectionCursor = db.collection(collection).find()
		
		console.log(`Worker #${process.pid}: Adding squareTileId to ${collection} collection`)

		return new Promise((resolve, reject) => {
			compareToTiles(squareTiles, 'properties.squareTileId')
				.then(() => {
					collectionCursor.rewind()
					console.log(`Worker #${process.pid}: Adding triangleTileId to ${collection} collection`)
					return compareToTiles(triangleTiles, 'properties.triangleTileId')
				})
				.then(() => {
					console.log(`Worker #${process.pid}: Done Adding both Tile IDs to ${collection} collection`)
					resolve(db)
				})
				.catch(err => { console.error(err) })
		})
	}
}

module.exports = new AddTilesToLayer()

function compareToTiles(tiles, tileIDProperty) {
	return new Promise((resolve, reject) => {
		iterateCollectionCursor((cb) => {
			resolve()
		})

		function iterateCollectionCursor(cb) {
			collectionCursor.nextObject((err, feature) => {
				if (feature && !err) {
					var featureCentroid = turf.centroid(feature)
					
					for (var i = tiles.length - 1; i >= 0; i--) {
						if (turf.inside(featureCentroid, tiles[i])) {
							db.collection(collection).update({ _id: feature._id }, { $set: { [tileIDProperty]: tiles[i].properties.tile_id } })
						}
					}
					
					iterateCollectionCursor(cb)
				} else cb() // No more items
			})
		}
	})
}