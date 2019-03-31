import { readFileSync, close, writeFileSync, writeFile, createReadStream } from 'fs';
import * as path from 'path';
import {Table, Null} from 'apache-arrow';
import * as moment from 'moment';
import * as math from 'mathjs';
import * as parser from 'fast-xml-parser';
import { parseHex } from './src/sbe19plusV2/parseHex';

// Sample Data
const dir = "./data/sbe19plusV2/";
const hexFileName = "PORT_CTD5048_DO1360CT1460Op302_Hauls_1to5_21May2016.hex";
const xmlconFileName = "SBE19plusV2_5048.xmlcon";

const hexFile = path.resolve(path.join(dir, hexFileName));
const xmlconFile = path.resolve(path.join(dir, xmlconFileName));

console.info(`hex file: ${hexFile}`);

async function bulkProcess() {
// const bulkProcess = async() => {

    // Find all of the hex files and associated xmlcon files
    // ToDo

    // Read an individiaul hex and xmlcon file
    // const hexFileInMemory = readFileSync(hexFile);
    const xmlconFileInMemory = readFileSync(xmlconFile, "utf8");

    // Retrieve the xmlcon instrument and sensor details as JSON
    const xmlconJson = parser.parse(xmlconFileInMemory);
    const instrument = xmlconJson.SBE_InstrumentConfiguration.Instrument;
    const sensors = instrument.SensorArray.Sensor;
    // console.info(instrument);
    // console.info(sensors);

    // Parse hex file and convert to uncorrected values in arrow data structure
    if (instrument.Name.indexOf("SBE 19plus V2") > -1) {
        // Parse the SBE 19plusV2 hex file
        console.info('Parsing SBE19plusV2 file');
        // const output  = await parseHex(hexFile);

        // (async () => {
        await parseHex(hexFile, instrument, sensors);
        // console.info(`output: ${JSON.stringify(output['casts'])}`)

            // for await (const line of parseHex(hexFile)) {
            //   console.log(line);
            // }
        //   })();



        // parseHex(hexFile).then((output) => {
        //     console.info(`output: ${JSON.stringify(output)}`)
        // });
    }

    // Auto QA/QC the new arrow data structure

    // Persist the data to disk

}

bulkProcess();