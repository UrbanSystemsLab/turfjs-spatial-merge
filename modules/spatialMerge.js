var turf = require('turf')

var squareTiles = []
var triangleTiles = []
var squareTileCursor = 0
var triangleTileCursor = 0
var db
var innerLayerCollection, outerLayerCollection
var spatialMergeAttributes

var SpatialMergeLayers = function() {
	this.init = function(_db, _innerLayerCollection, _outerLayerCollection, _squareTiles, _triangleTiles, _spatialMergeAttributes) {
		squareTiles = _squareTiles
		triangleTiles = _triangleTiles
		db = _db
		innerLayerCollection = _innerLayerCollection
		outerLayerCollection = _outerLayerCollection
		spatialMergeAttributes = _spatialMergeAttributes
		return new Promise((resolve, reject) => {
			console.log(`Worker #${process.pid}: Merging attributes inside square tiles`)
			startCompare((err, res) => {
				if (!err) resolve(db)
				else reject(err)
			})
		})
	}
}
module.exports = new SpatialMergeLayers()

// ------------------------

function startCompare(cbMerge) {
	compareLayers(squareTiles, squareTileCursor, 'properties.squareTileId', (cbTile) => {
		console.log(`Worker #${process.pid}: Merging attributes inside triangle tiles`)
		compareLayers(triangleTiles, triangleTileCursor, 'properties.triangleTileId', (cbTile) => {
			cbMerge(null, null)
		})
	})
}

function compareLayers(tiles, tileCursor, tileIDProperty, cbTile) {
	var currentTile = tiles[tileCursor]
	var allOuterFeatures = []
	var allInnerFeatures = []

	db.collection(outerLayerCollection).find({ [tileIDProperty]: currentTile.properties.tile_id }).toArray()
		.then(_outerFeatures => {
			allOuterFeatures = _outerFeatures.slice(0)
			return db.collection(innerLayerCollection).find({ [tileIDProperty]: currentTile.properties.tile_id }).toArray()
		})
		.then(_innerFeatures => {
			allInnerFeatures = _innerFeatures.slice(0)
			return null
		})
		.then(() => {
			return allOuterFeatures.forEach(outerFeature => {
				allInnerFeatures.forEach(innerFeature => {
					var innerFeatureCentroid = turf.centroid(innerFeature)
					if (turf.inside(innerFeatureCentroid, outerFeature)) {
						// Merge attributes listed in config.json
						spatialMergeAttributes.forEach(attribute => {
							if (outerFeature.properties[attribute]) {
								db.collection(innerLayerCollection).update({ _id: innerFeature._id }, { $set: { [`properties.${attribute}`]: outerFeature.properties[attribute] } })
							}
						})
					}
				})
			})
		})
		.then(() => {
			tileCursor++
			if (tileCursor <= tiles.length - 1) {
				setTimeout(function() {
					compareLayers(tiles, tileCursor, tileIDProperty, cbTile)
				}, 0)
			} else {
				cbTile()
			}
		})
}