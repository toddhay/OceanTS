import { readFileSync, writeFileSync, readdirSync, mkdirSync, existsSync } from 'fs';
import * as path from 'path';
import * as os from 'os';
import {Table, Null} from 'apache-arrow';
import * as parser from 'fast-xml-parser';
import { parseHex } from './src/sbe19plusV2/parseHex';
import { convertToEngineeringUnits } from './src/sbe19plusV2/convertResults';
import { getTrawlSurveyHaulData, getHexFiles, getXmlconFiles, saveToFile } from './src/utilities';
import { logger } from './src/logger';
import * as moment from 'moment';

logger.info('***** Start data processing.... *****');
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

let start: moment.Moment = null, end: moment.Moment = null, duration: number = null,
    hex_file_start: moment.Moment = null;
let df: Table = null, results: Object = null, idx: number = 0;
let currentHex: string = null, currentXmlcon: string = null, 
    currentYear: string = null, currentVessel: string = null,
    currentPosition: string = null, currentCtd: string = null;
let strippedArray: string[] = null, lineArray: string[] = null, 
    hexFileArray: string[] = null;

async function bulkProcess() {

    // Retrieve the Trawl Survey Haul Data
    logger.info(`Retrieving haul data`)
    start = moment();
    const hauls = await getTrawlSurveyHaulData();
    end = moment();
    duration = moment.duration(end.diff(start)).asSeconds();
    logger.info(`\tProcessing time - retrieving haul data: ${duration}s`);

    // Find all of the hex files
    logger.info(`Searching for hex files: ${dataDir}`);
    start = moment();
    let hexFilesArray = await getHexFiles(dataDir);
    writeFileSync(path.join(os.homedir(), "Desktop", "hexFiles.txt"),
        hexFilesArray.join("\n")
    );
    logger.info(`\thex file count: ${hexFilesArray.length}`);
    end = moment();
    duration = moment.duration(end.diff(start)).asSeconds();
    logger.info(`\tProcessing time - getting hex files: ${duration}s`);

    // Find all of the xmlcon files
    logger.info(`Searching for xmlcon files: ${dataDir}`);
    start = moment();
    let xmlconFilesArray = await getXmlconFiles(dataDir);
    writeFileSync(path.join(os.homedir(), "Desktop", "xmlconList.txt"),
        xmlconFilesArray.join("\n")
    );
    logger.info(`\txmlcon file count: ${xmlconFilesArray.length}`);
    end = moment();
    duration = moment.duration(end.diff(start)).asSeconds();
    logger.info(`\tProcessing time - getting xmlcon files: ${duration}s`);

    // Prepare hex file list for parsing
    strippedArray = hexFilesArray.map(x => {
        return x.replace(dataDir.replace(/\\/g, '\/') + "/", "");
    });

    // TESTING ONLY
    strippedArray = strippedArray.slice(0,2);

    // Must use for ... of syntax for proper ordering, per:  
    //     https://lavrton.com/javascript-loops-how-to-handle-async-await-6252dd3c795/
    for (const x of strippedArray) {

        hex_file_start = moment();

        lineArray = x.split("/");
        currentYear = lineArray[0];
        currentVessel = lineArray[1]; 
        hexFileArray = lineArray.slice(-1)[0].split("_");
        currentPosition = hexFileArray[0];
        currentCtd = hexFileArray[1].replace("CTD", "");

        console.info("\n");
        logger.info(`*** Processing item ${idx}: ${currentYear}, ${currentVessel}, ` +
            `${currentPosition}, ${currentCtd} ***`);

        currentHex = path.resolve(hexFilesArray[idx]);
        currentXmlcon = path.resolve(path.join(dataDir, currentYear, 
            currentYear + "_CTD_ConFiles_Raw", "SBE19plusV2_" + currentCtd + ".xmlcon"))

        // Read an individiaul xmlcon file
        let xmlconFileInMemory = readFileSync(currentXmlcon, "utf8");

        // Retrieve the xmlcon instrument and sensor details as JSON
        let xmlconJson = parser.parse(xmlconFileInMemory);
        let instrument = xmlconJson.SBE_InstrumentConfiguration.Instrument;
        let sensors = instrument.SensorArray.Sensor;

        let outputFile = path.join(outputDir, lineArray.slice(-1)[0].slice(0, -3) + "csv");


        // Parse hex file and convert to raw, decimal values in arrow data structure
        if (instrument.Name.indexOf("SBE 19plus V2") > -1) {

            logger.info(`\txmlcon: ${currentXmlcon}`);
            logger.info(`\tinputHex = ${currentHex}`);
            logger.info(`\toutputCSV = ${outputFile}`);
    
            // results = await parseHex(currentHex, instrument, sensors, outputFile, hauls, currentVessel);
            logger.info(`\tParsing Hex File`);
            start = moment();        
            results = await parseHex(currentHex);
            end = moment();
            duration = moment.duration(end.diff(start)).asSeconds();
            logger.info(`\t\tProcessing time - parsing hex file: ${duration}s`);

            logger.info(`\tConverting to Engineering Units`);
            start = moment();        
            df = await convertToEngineeringUnits(instrument, sensors, results["casts"], 
                results["voltageOffsets"], results["pumpDelay"], results["df"], 
                outputFile, hauls, currentVessel);
            end = moment();
            duration = moment.duration(end.diff(start)).asSeconds();
            logger.info(`\t\tProcessing time - converting to engineering units: ${duration}s`);

            // Save the results to a csv file
            logger.info(`\tSaving data to a csv file`);
            start = moment();
            let outputColumns = ["Temperature (degC)", "Pressure (dbars)", "Conductivity (S_per_m)",
                "Salinity (psu)", "Oxygen (ml_per_l)", "OPTODE Oxygen (ml_per_l)", "Depth (m)",
                "Latitude (decDeg)", "Longitude (decDeg)", "HaulID", "DateTime (ISO8601)", "Year", "Month", "Day"
            ];
            await saveToFile(df, "csv", outputFile, outputColumns);
            end = moment();
            duration = moment.duration(end.diff(start)).asSeconds();
            logger.info(`\t\tProcessing time - saving result to a file: ${duration}s`);

            // Display the results
            // let sliceSize: number = 5, 
            //     sliceStart: number = 0, //df.length - sliceSize, 
            //     sliceEnd: number = sliceStart + sliceSize;
            // try {
                // outputColumns.forEach(x => {
                //     results = df.getColumn(x).toArray().slice(sliceStart, sliceEnd);
                //     console.info(`\t${x}: ${results}`);
                // });
                // console.info(`Schema: ${df.schema.fields.map(x => x.name)}`);
                // console.info(`Voltage Offsets: ${JSON.stringify(voltageOffsets)}`);
                // console.info(`Casts: ${JSON.stringify(casts)}`);
            // } catch (e) {
            //     logger.error(`Error printing results: ${e}`);
            // }
        }
        end = moment();
        duration = moment.duration(end.diff(hex_file_start)).asSeconds();
        logger.info(`\tProcessing time - item ${idx} - overall file processing: ${duration}s`);
        
        idx += 1;
    }
    // })

    // ToDo - Auto QA/QC the new arrow data structure

    // ToDo - Persist the data to disk

}

bulkProcess();