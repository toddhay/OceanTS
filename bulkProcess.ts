import { readFileSync, writeFileSync, readdirSync, mkdirSync, existsSync } from 'fs';
import * as path from 'path';
import * as os from 'os';
import {Table, Null} from 'apache-arrow';
import * as parser from 'fast-xml-parser';
import { parseHex } from './src/sbe19plusV2/parseHex';
import { getTrawlSurveyHaulData, getHexFiles, getXmlconFiles } from './src/utilities';
import { logger } from './src/logger';
import * as moment from 'moment';

// Set up log4js logger
// import logjson from './src/log.json';
// import { configure, getLogger } from 'log4js';
// export const logger = getLogger();
// configure(logjson);
// logger.level = "debug";
logger.info('Start data processing....');

// logger.fatal('big error');
// process.exit(0);
// setInterval(function(){ process.exit(0); }, 100);

// Sample Data
const dir = "./data/sbe19plusV2/";
const hexFileName = "PORT_CTD5048_DO1360CT1460Op302_Hauls_1to5_21May2016.hex";
const xmlconFileName = "SBE19plusV2_5048.xmlcon";

const dataDir = path.join(os.homedir(), "Desktop", "CTD");  // Change to the real dir for processing
const outputDir = path.join(os.homedir(), "Desktop", "CTD output");
if (!existsSync(outputDir)) {
    mkdirSync(outputDir);
}

let start: any = null, end: any = null, duration: any = null;

async function bulkProcess() {

    // Retrieve the Trawl Survey Haul Data
    console.info(`Retrieving haul data`)
    start = moment();
    const hauls = await getTrawlSurveyHaulData();
    end = moment();
    duration = moment.duration(end.diff(start)).asSeconds();
    console.info(`\tProcessing time - retrieving haul data: ${duration}s`);

    // Find all of the hex files
    console.info(`Searching for hex files: ${dataDir}`);
    start = moment();
    let hexFilesArray = await getHexFiles(dataDir);
    writeFileSync(path.join(os.homedir(), "Desktop", "hexFiles.txt"),
        hexFilesArray.join("\n")
    );
    console.info(`\thex file count: ${hexFilesArray.length}`);
    end = moment();
    duration = moment.duration(end.diff(start)).asSeconds();
    console.info(`\tProcessing time - getting hex files: ${duration}s`);

    // Find all of the xmlcon files
    console.info(`Searching for xmlcon files: ${dataDir}`);
    start = moment();
    let xmlconFilesArray = await getXmlconFiles(dataDir);
    writeFileSync(path.join(os.homedir(), "Desktop", "xmlconList.txt"),
        xmlconFilesArray.join("\n")
    );
    console.info(`\txmlcon file count: ${xmlconFilesArray.length}`);
    end = moment();
    duration = moment.duration(end.diff(start)).asSeconds();
    console.info(`\tProcessing time - getting xmlcon files: ${duration}s`);

    let currentHex: string = null, currentXmlcon: string = null, 
        currentYear: string = null, currentVessel: string = null,
        currentPosition: string = null, currentCtd: string = null;
    let strippedArray: string[] = null, lineArray: string[] = null, 
        hexFileArray: string[] = null;
    strippedArray = hexFilesArray.map(x => {
        return x.replace(dataDir.replace(/\\/g, '\/') + "/", "");
    });
    await strippedArray.forEach(async (x: any, idx: number) => {
        // if (idx === 5) process.exit(0);

        lineArray = x.split("/");
        currentYear = lineArray[0];
        currentVessel = lineArray[1]; 
        hexFileArray = lineArray.slice(-1)[0].split("_");
        currentPosition = hexFileArray[0];
        currentCtd = hexFileArray[1].replace("CTD", "");

        start = moment();
        console.info(`Processing: ${currentYear}, ${currentVessel}, ${currentPosition}, ` +
            `${currentCtd} - ${lineArray.slice(-1)[0]}`);

        currentHex = path.resolve(hexFilesArray[idx]);
        currentXmlcon = path.resolve(path.join(dataDir, currentYear, 
            currentYear + "_CTD_ConFiles_Raw", "SBE19plusV2_" + currentCtd + ".xmlcon"))
        console.info(`\txmlcon: ${currentXmlcon}`);

        // Read an individiaul xmlcon file
        let xmlconFileInMemory = readFileSync(currentXmlcon, "utf8");

        // Retrieve the xmlcon instrument and sensor details as JSON
        let xmlconJson = parser.parse(xmlconFileInMemory);
        let instrument = xmlconJson.SBE_InstrumentConfiguration.Instrument;
        let sensors = instrument.SensorArray.Sensor;

        let outputFile = path.join(outputDir, lineArray.slice(-1)[0].slice(0, -3) + "csv");
        console.info(`\tinputHex = ${currentHex}`);
        console.info(`\toutputFile = ${outputFile}`);

        // Parse hex file and convert to raw, decimal values in arrow data structure
        if (instrument.Name.indexOf("SBE 19plus V2") > -1) {
            await parseHex(currentHex, instrument, sensors, outputFile, hauls, currentVessel);
            console.info(`\tafter await`);
        }
        end = moment();
        duration = moment.duration(end.diff(start)).asSeconds();
        console.info(`\tProcessing time - overall file processing: ${duration}s`);
    
    })

    // ToDo - Auto QA/QC the new arrow data structure

    // ToDo - Persist the data to disk

}

bulkProcess();