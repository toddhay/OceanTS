import { readFileSync, writeFileSync, readdirSync } from 'fs';
import * as path from 'path';
import * as os from 'os';
import {Table, Null} from 'apache-arrow';
import * as parser from 'fast-xml-parser';
import { parseHex } from './src/sbe19plusV2/parseHex';
import { getTrawlSurveyHaulData, getHexFiles, getXmlconFiles } from './src/utilities';
import moment = require('moment');

// Sample Data
const dir = "./data/sbe19plusV2/";
const hexFileName = "PORT_CTD5048_DO1360CT1460Op302_Hauls_1to5_21May2016.hex";
const xmlconFileName = "SBE19plusV2_5048.xmlcon";

const dataDir = path.join(os.homedir(), "Desktop", "CTD");  // Change to the real dir for processing

let start: any = null, end: any = null;

async function bulkProcess() {

    start = moment();

    // Retrieve the Trawl Survey Haul Data
    const hauls = await getTrawlSurveyHaulData();
    console.info(`first row: ${hauls.get(0)}`)
    console.info(`schema: ${hauls.schema.fields.map(x => x.name)}`);
    // process.exit(0);

    // ToDo - Find all of the hex files and associated xmlcon files
    console.info(`searching for hex: ${dataDir}`);
    let hexFiles = await getHexFiles(dataDir);
    writeFileSync(path.join(os.homedir(), "Desktop", "hexFiles.txt"),
        hexFiles.toString().split(",").join("\n")
    );
    console.info(`hex file count: ${hexFiles.length}`);
    end = moment();
    let duration = moment.duration(end.diff(start)).asSeconds();
    console.info(`Processing time: ${duration}s`);

    // Find all of the xmlcon files
    console.info(`searching for xmlcon: ${dataDir}`);
    let xmlconFiles = await getXmlconFiles(dataDir);
    writeFileSync(path.join(os.homedir(), "Desktop", "xmlconList.txt"),
        xmlconFiles.toString().split(",").join("\n")
    );
    console.info(`xmlcon file count: ${xmlconFiles.length}`);
    end = moment();
    duration = moment.duration(end.diff(start)).asSeconds();
    console.info(`Processing time: ${duration}s`);

    const hexFile = path.resolve(path.join(dir, hexFileName));
    const xmlconFile = path.resolve(path.join(dir, xmlconFileName));
    console.info(`hex file: ${hexFile}`);

    // Read an individiaul xmlcon file
    const xmlconFileInMemory = readFileSync(xmlconFile, "utf8");

    // Retrieve the xmlcon instrument and sensor details as JSON
    const xmlconJson = parser.parse(xmlconFileInMemory);
    const instrument = xmlconJson.SBE_InstrumentConfiguration.Instrument;
    const sensors = instrument.SensorArray.Sensor;

    // Parse hex file and convert to raw, decimal values in arrow data structure
    if (instrument.Name.indexOf("SBE 19plus V2") > -1) {

        // Parse the SBE 19plusV2 hex file
        console.info('Parsing SBE19plusV2 file');
        await parseHex(hexFile, instrument, sensors, hauls);
    }

    // ToDo - Auto QA/QC the new arrow data structure

    // ToDo - Persist the data to disk

}

bulkProcess();