var turf = require('turf')

var collectionCursor
var boundaries = []
var db, collection

var ClipFeatures = function() {
	this.init = function(db, boundaries, collection) {
		console.log(`Worker #${process.pid}: Clipping ${collection} features to within city Boundary`)
		return new Promise((resolve, reject) => {
			startCompare(db, boundaries, collection, (err, res) => {
				if (!err) resolve(db)
				else reject(err)
			})
		})
	}
}

module.exports = new ClipFeatures()

function startCompare(_db, _boundaries, _collection, callback) {
	boundaries = _boundaries // Make the boundaries available globally.
	db = _db
	collection = _collection
	collectionCursor = db.collection(collection).find()
	iterateCollectionCursor((cb) => {
		callback(null, null)
	})
}

function iterateCollectionCursor(cb) {
	collectionCursor.nextObject((err, feature) => {
		if (feature && !err) {
			var featureCentroid = turf.centroid(feature)
			for (var i = boundaries.length - 1; i >= 0; i--) {
				if (turf.inside(featureCentroid, boundaries[i])) {
					// --
					feature.properties.borough = getBoroughCode(boundaries[i])	// Specific for New York. Modify as per requirement
					// --
					db.collection(`${collection}_temp`).insert(feature) // [Collection]_temp renamed to [Collection] after each worker finishes
				}
			}
			iterateCollectionCursor(cb)
		} else {
			cb() // No more items
		}
	})
}

function getBoroughCode(boundary) {
	switch (boundary.properties.boro_name) {
		case 'Queens':
			return 'QN'
		case 'Manhattan':
			return 'MN'
		case 'Bronx':
			return 'BX'
		case 'Brooklyn':
			return 'BK'
		case 'Staten Island':
			return 'SI'
		default:
			return null
	}
}
