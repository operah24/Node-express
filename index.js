const express = require('express');
const bodyParser = require('body-parser');

const uuid = require('uuid');

const app = express();

// create application/json parser
var jsonParser = bodyParser.json()

app.use(bodyParser.json({ limit: '50mb' }));
app.use(bodyParser.urlencoded({ extended: false }));

const port = 3002;

/**
 * Get
 */
app.get('/', async (req, res) => {
    res.json({ "about": "CSV Parser Developed by Opeyemi Bantale" })
});

/**
 * POST
 * Parses the CSV file and return in specified format.
 */
app.post('/', async (req, res) => {

    let body = req.body;

    let csvUrl = body.csv.url;

    try {

        if (!csvUrl.includes('.csv')) {
            res.json({ error: "Invalid file extension" });
        }

        let csvData = await getCSVData(csvUrl);

        let obj = {};

        if (body.csv.select_fields) {

            for (let i = 0; i < body.csv.select_fields.length; i++) {
                obj[body.csv.select_fields[i]] = '';
            }

            let missingKeys = [];
            for (const key in csvData[0]) {

                if (obj[key] === undefined & obj[key] !== '') {
                    missingKeys.push(key);
                }
            }

            for (let k = 0; k < missingKeys.length; k++) {
                for (let a = 0; a < csvData.length; a++) {
                    delete csvData[a][missingKeys[k]];
                }
            }

            res.json({ conversion_key: uuid.v1().replace(/-/g, ""), json: csvData });

        } else {
            res.json({ conversion_key: uuid.v1().replace(/-/g, ""), json: csvData });
        }

    } catch (error) {
        res.json({ error: "Invalid CSV Format." })
    }

});

app.listen(port, () => {
    console.log(`CSV Parser app listening at http://localhost:${port}`)
});


/**
 * Fetches the CSV data file from the CSV Url and return the data in JSON format.
 * @param {*} csvUrl 
 * 
 */

function getCSVData(csvUrl) {

    return new Promise((resolve, reject) => {

        const fs = require('fs');
        const path = require('path');
        const csv = require('fast-csv');

        fs.appendFileSync('data.csv', '');

        const https = require('https');

        const file = fs.createWriteStream("data.csv");
        https.get(csvUrl, function (response) {
            response.pipe(file);

            let csvData = [];

            fs.createReadStream(path.resolve(__dirname, 'data.csv'))
                .pipe(csv.parse({ headers: true }))
                .on('error', error => {
                    console.error(error);
                    reject(error);
                })
                .on('data', row => {
                    csvData.push(row);
                })
                .on('end', rowCount => {
                    resolve(csvData);
                });
        });

    });

}