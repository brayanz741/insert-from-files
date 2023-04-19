require('dotenv').config();
const fs = require('fs');
const mysql = require('mysql2/promise');

const folderPath = './files/';
const ignoreFile = ['.gitignore'];
const fieldDelimiter = ',';
const lineDelimiter = '\n';
const recordsByInsert = 20000;
const connectionData = {
	host: process.env.HOSTDB,
	port: process.env.PORTDB,
	user: process.env.USERDB,
	password: process.env.PASSDB,
	database: process.env.DATABASE,
};

async function main() {
	console.log('Obteniendo Archivos');
	let files = await getFiles(folderPath);
	for (const file of files) {
		console.log('Obteniendo los datos del archivo ', file);
		let data = await getDataFromFile(file);
		if (data.length < 1) continue;
		console.log('Extrayendo solo los valores unicos de códigos');
		let uniqueCodes = await getUniqueValues(data, 'CAMPO', 'name');
		const connection = await createConnection();
		console.log(`Insertando ${uniqueCodes.length} Registros, del archivo ${file}`);
		await insertMultipleRecordsInBatches(connection, 'codes', uniqueCodes, recordsByInsert);
	}
	process.exit(0);
}

async function getFiles(path) {
	return (fileList = fs.readdirSync(path));
}

async function createFile(pathFile, data) {
	fs.writeFileSync(pathFile, data);
}

async function getDataFromFile(file) {
	if (ignoreFile.includes(file)) return [];
	let content = fs.readFileSync(folderPath + file, { encoding: 'utf8', flag: 'r' });
	let lines = content.trim().split(lineDelimiter);
	const headers = lines.shift().split(fieldDelimiter); // Obtener las cabeceras del archivo
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
	const values = objects.map((obj) => obj[property]);
	const uniqueValues = values.filter((value, index) => {
		return values.indexOf(value) === index;
	});
	const result = uniqueValues.map((value) => {
		const obj = {};
		obj[newPropertyName] = value;
		return obj;
	});
	return result;
}

async function createConnection() {
	const connection = await mysql.createPool(connectionData);
	return connection;
}

async function insertMultipleRecordsInBatches(connection, tableName, data, segmentSize) {
	const segmentCount = Math.ceil(data.length / segmentSize);
	let insertedRows = 0;

	for (let i = 0; i < segmentCount; i++) {
		const segment = data.slice(i * segmentSize, (i + 1) * segmentSize);
		const keys = Object.keys(segment[0]).join(', ');
		const values = segment
			.map(
				(item) =>
					`(${Object.values(item)
						.map((val) => `'${val}'`)
						.join(', ')})`
			)
			.join(', ');
		const query = `INSERT INTO ${tableName} (${keys}) VALUES ${values}`;

		const result = await connection.query(query);
		insertedRows += result.affectedRows;
	}

	return insertedRows;
}

main();
