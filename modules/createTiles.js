var Promise = require('bluebird')
const turf = require('turf')

var tileCount = 0
var boundaries = null
var boundaryCollection = null
var db = null

var CreateTiles = function() {
	this.init = function(_db, _boundaryCollection) {
		db = _db
		boundaryCollection = _boundaryCollection

		return new Promise((resolve, reject) => {
			boundaries = db.collection(boundaryCollection).find().toArray()
			if (boundaries) {
				addGrid('squareGrid', 1)
					.then(() => {
						return addGrid('triangleGrid', 1.77)
					}).then(() => {
						// Haha. Bad Idea. 
						setTimeout(function() {
							console.log('Completed adding square and triangle grid')
							process.exit()
						}, 5000)
					})
			} else {
				console.error('Error: No boundaries to clip tiles from')
				process.exit()
			}
		})
	}
}

module.exports = new CreateTiles()

function addGrid(gridType, tileSize) {
	return Promise.map(boundaries, (boundary) => {
		var bbox = turf.bbox(boundary)
		var cellSize = tileSize
		var units = 'miles'
		var grid

		if (gridType === 'squareGrid') {
			grid = turf.squareGrid(bbox, cellSize, units)
		} else if (gridType === 'triangleGrid') {
			grid = turf.triangleGrid(bbox, cellSize, units)
		}

		grid.features.forEach(tile => {
			tile = turf.intersect(boundary, tile)

			if (tile) {
				// -- MODIFY tile.properties AS PER REQUIREMENT
				tile.properties.borough = getBoroughCode(boundary)
				// --

				tile.properties.tile_id = tileCount
				tileCount++
				return db.collection(gridType).insert(tile)
			} else {
				return null
			}
		})
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
