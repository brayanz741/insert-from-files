require('dotenv').config();
const fs = require('fs');
const mysql = require('mysql2/promise');

const folderPath = './files/';
const ignoreFile = ['.gitignore'];
const fieldDelimiter = ',';
const lineDelimiter = '\n';
const recordsByInsert = 20000;
const tableName = 'codes';
const connectionData = {
	host: process.env.HOSTDB,
	port: process.env.PORTDB,
	user: process.env.USERDB,
	password: process.env.PASSDB,
	database: process.env.DATABASE,
};

async function main() {
	console.log('Obteniendo Archivos');
	const files = await getFiles(folderPath);
	for (const file of files) {
		console.time(file);
		const data = await getDataFromFile(file);
		if (data.length < 1) continue;
		const uniqueCodes = await getUniqueValues(data, 'CAMPO', 'name');
		const connection = await createConnection();
		await insertMultipleRecordsInBatches(connection, uniqueCodes, recordsByInsert);
		connection.end();
		console.timeEnd(file);
	}
	process.exit(0);
}

async function getFiles(path) {
	return fs.promises.readdir(path);
}

async function getDataFromFile(file) {
	if (ignoreFile.includes(file)) return [];
	const content = await fs.promises.readFile(folderPath + file, { encoding: 'utf8', flag: 'r' });
	const lines = content.trim().split(lineDelimiter);
	const headers = lines.shift().split(fieldDelimiter);
	const obj = lines.map((line) => {
		const values = line.split(fieldDelimiter);
		return headers.reduce((obj, header, i) => {
			obj[header] = values[i];
			return obj;
		}, {});
	});
	return obj;
}

async function getUniqueValues(objects, property, newPropertyName) {
	const uniqueValues = new Set(objects.map((obj) => obj[property]));
	const result = [];
	for (const value of uniqueValues) {
		const obj = {};
		obj[newPropertyName] = value;
		result.push(obj);
	}
	return result;
}

async function createConnection() {
	return mysql.createPool(connectionData);
}

async function insertMultipleRecordsInBatches(connection, data, segmentSize) {
	const segmentCount = Math.ceil(data.length / segmentSize);
	let insertedRows = 0;

	for (let i = 0; i < segmentCount; i++) {
		const segment = data.slice(i * segmentSize, (i + 1) * segmentSize);
		const keys = Object.keys(segment[0]).join(', ');
		const values = segment
			.map(
				(item) =>
					`(${Object.values(item)
						.map((val) => mysql.escape(val))
						.join(', ')})`
			)
			.join(', ');
		const query = `INSERT INTO ${tableName} (${keys}) VALUES ${values}`;
		const result = await connection.query(query);
		insertedRows += result[0].affectedRows;
	}

	return insertedRows;
}

main();
