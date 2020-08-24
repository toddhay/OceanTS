import { readFileSync, writeFileSync, readdirSync, mkdirSync, existsSync, createReadStream } from 'fs';
import * as path from 'path';
import * as os from 'os';
import {Table, Float32Vector, DateVector, Int32Vector, Utf8Vector} from 'apache-arrow';
import * as parser from 'fast-xml-parser';
import { parseHex } from './src/sbe19plusV2/parseHex';
import { convertToEngineeringUnits } from './src/sbe19plusV2/convertResults';
import { getTrawlSurveyHaulData, getHexFiles, getCsvFiles, getXmlconFiles, saveToFile } from './src/utilities';
import { logger } from './src/logger';
import * as moment from 'moment';
import { removeOutliers } from './src/outlierRemoval';
import { createInterface } from 'readline';
import * as csv from 'csv';
import * as papa from './sandbox/structures/node_modules/papaparse';
import { splitHauls, parseFile, csvToTable, sliceByTimeRange, slice } from './sandbox/structures';
import { col } from 'apache-arrow/compute/predicate';

logger.info('***** Start data processing.... *****');
// process.exit(0);
// setInterval(function(){ process.exit(0); }, 100);

// 
let dataStruct = [
    { "colName": "Temperature (degC)", "vectorType": Float32Vector, "vector": null },
    { "colName": "Pressure (dbars)", "vectorType": Float32Vector, "vector": null },
    { "colName": "Conductivity (S_per_m)", "vectorType": Float32Vector, "vector": null },
    { "colName": "Salinity (psu)", "vectorType": Float32Vector, "vector": null },
    { "colName": "Oxygen SBE43 (ml_per_l)", "vectorType": Float32Vector, "vector": null },
    { "colName": "Oxygen OPTODE (ml_per_l)", "vectorType": Float32Vector, "vector": null },
    { "colName": "Depth (m)", "vectorType": Float32Vector, "vector": null },
    { "colName": "Latitude (decDeg)", "vectorType": Float32Vector, "vector": null },
    { "colName": "Longitude (decDeg)", "vectorType": Float32Vector, "vector": null },
    { "colName": "HaulID", "vectorType": Utf8Vector, "vector": null },
    { "colName": "DateTime (ISO8601)", "vectorType": DateVector, "vector": null },
    { "colName": "Year", "vectorType": Int32Vector, "vector": null },
    { "colName": "Month", "vectorType": Int32Vector, "vector": null },
    { "colName": "Day", "vectorType": Int32Vector, "vector": null },
];
let outputColumns = dataStruct.map((x: any) => {
    return x["colName"];
})

// let outputColumns = ["Temperature (degC)", "Pressure (dbars)", "Conductivity (S_per_m)",
// "Salinity (psu)", "Oxygen (ml_per_l)", "OPTODE Oxygen (ml_per_l)", "Depth (m)",
// "Latitude (decDeg)", "Longitude (decDeg)", "HaulID", "DateTime (ISO8601)", "Year", "Month", "Day"
// ];


// Sample Data
const dir = "./data/sbe19plusV2/";
const hexFileName = "PORT_CTD5048_DO1360CT1460Op302_Hauls_1to5_21May2016.hex";
const xmlconFileName = "SBE19plusV2_5048.xmlcon";

let hexDir = path.join(os.homedir(), "Desktop", "CTD");  // Change to the real dir for processing
// hexDir = path.join(os.homedir(), "Desktop", "CTD_Test");  // Change to the real dir for processing

const csvDir = path.join(os.homedir(), "Desktop", "CTD output");
if (!existsSync(csvDir)) {
    mkdirSync(csvDir);
}
const pointEstimatesDir = path.join(os.homedir(), "Desktop", "Point Estimates");
if (!existsSync(pointEstimatesDir)) {
    mkdirSync(pointEstimatesDir);
}

let currentOutputDir: string = null;
let start: moment.Moment = null, end: moment.Moment = null, duration: number = null,
    hex_file_start: moment.Moment = null, bulkStart: moment.Moment = null, bulkEnd: moment.Moment = null;
let df: Table = null, results: Object = null;
let currentHex: string = null, currentXmlcon: string = null, 
    currentYear: string = null, currentVessel: string = null,
    currentPosition: string = null, currentCTD: string = null;
let strippedArray: string[] = null, lineArray: string[] = null, 
    hexFileArray: string[] = null;
let metrics: number[] = [];

async function bulkConvertData() {

    // Retrieve the Trawl Survey Haul Data
    logger.info(`Retrieving haul data`)
    start = moment();
    let startYear = '2016';
    let endYear = '2018'
    const hauls = await getTrawlSurveyHaulData(startYear, endYear);
    end = moment();
    duration = moment.duration(end.diff(start)).asSeconds();
    logger.info(`\tProcessing time - retrieving haul data: ${duration}s`);

    // Find all of the hex files
    logger.info(`Searching for hex files: ${hexDir}`);
    start = moment();
    let hexFilesArray = await getHexFiles(hexDir);
    writeFileSync(path.join(os.homedir(), "Desktop", "hexFiles.txt"),
        hexFilesArray.join("\n")
    );
    logger.info(`\thex file count: ${hexFilesArray.length}`);
    end = moment();
    duration = moment.duration(end.diff(start)).asSeconds();
    logger.info(`\tProcessing time - getting hex files: ${duration}s`);

    // Find all of the xmlcon files
    logger.info(`Searching for xmlcon files: ${hexDir}`);
    start = moment();
    let xmlconFilesArray = await getXmlconFiles(hexDir);
    writeFileSync(path.join(os.homedir(), "Desktop", "xmlconList.txt"),
        xmlconFilesArray.join("\n")
    );
    logger.info(`\txmlcon file count: ${xmlconFilesArray.length}`);
    end = moment();
    duration = moment.duration(end.diff(start)).asSeconds();
    logger.info(`\tProcessing time - getting xmlcon files: ${duration}s`);

    // Prepare hex file list for parsing
    strippedArray = hexFilesArray.map(x => {
        return x.replace(hexDir.replace(/\\/g, '\/') + "/", "");
    });

    // TESTING ONLY
    // strippedArray = strippedArray.slice(0, 3);

    let idx: number = 0, outputFile: string = null;
    // Must use for ... of syntax for proper ordering, per:  
    //     https://lavrton.com/javascript-loops-how-to-handle-async-await-6252dd3c795/

    // Iterate through all of the hex files and convert to csv's
    for await (const x of strippedArray) {

        // if (idx < 295) {
        //     idx += 1;
        //     continue;
        // }
        // if (idx === 300) break;

        hex_file_start = moment();

        lineArray = x.split("/");
        hexFileArray = lineArray.slice(-1)[0].split("_");

        currentYear = lineArray[0];
        currentVessel = lineArray[1]; 
        currentPosition = hexFileArray[0];
        currentCTD = hexFileArray[1].replace("CTD", "");

        // Only process 2017 data - Process everyting but the 2017 CTD7738 system
        // if (  (currentYear === "2016") ||  
        //     ((currentYear === "2017") && (currentCTD === "7738"))
        //     ) {
        //     idx += 1;
        //     continue;
        // }

        // Create the output directory if it does not exist + outputFile string
        if (!existsSync(path.join(csvDir, currentYear)))
            mkdirSync(path.join(csvDir, currentYear));
        if (!existsSync(path.join(csvDir, currentYear, currentVessel)))
            mkdirSync(path.join(csvDir, currentYear, currentVessel));
        currentOutputDir = path.join(csvDir, currentYear, currentVessel);
        outputFile = path.join(currentOutputDir, lineArray.slice(-1)[0].slice(0, -3) + "csv");

        console.info("\n");
        logger.info(`**************************************************`);
        logger.info(`*** Processing item ${idx}: ${currentYear}, ${currentVessel}, ` +
            `${currentPosition}, ${currentCTD} ***`);
        logger.info(`**************************************************`);

        currentHex = path.resolve(path.join(hexDir, strippedArray[idx]));
        currentXmlcon = path.resolve(path.join(hexDir, currentYear, 
            currentYear + "_CTD_ConFiles_Raw", "SBE19plusV2_" + currentCTD + ".xmlcon"))
            
        logger.info(`\txmlcon: ${currentXmlcon}`);
        logger.info(`\tinputHex = ${currentHex}`);
        logger.info(`\toutputCSV = ${outputFile}`);

        // TESTING ONLY
        // idx += 1;
        // continue;

        // Read an individiaul xmlcon file
        let xmlconFileInMemory = readFileSync(currentXmlcon, "utf8");

        // Retrieve the xmlcon instrument and sensor details as JSON
        let xmlconJson = parser.parse(xmlconFileInMemory);
        let instrument = xmlconJson.SBE_InstrumentConfiguration.Instrument;
        let sensors = instrument.SensorArray.Sensor;

        // Parse hex file and convert to raw, decimal values in arrow data structure
        if (instrument.Name.indexOf("SBE 19plus V2") > -1) {
    
            // Parse the hex file
            logger.info(`\tParsing Hex File`);
            start = moment();     
            results = await parseHex(currentHex);
            end = moment();
            duration = moment.duration(end.diff(start)).asSeconds();
            logger.info(`\t\tProcessing time - parsing hex file: ${duration}s`);

            // Convert the parsed hex file data to engineering unitst
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
            await saveToFile(df, "csv", outputFile, outputColumns);
            end = moment();
            duration = moment.duration(end.diff(start)).asSeconds();
            logger.info(`\t\tProcessing time - saving result to a file: ${duration}s`);

            // Display the results
            // let sliceSize: number = 5, 
            //     sliceStart: number = 0, //df.length - sliceSize, 
            //     sliceEnd: number = sliceStart + sliceSize;
            // try {
            //     outputColumns.forEach(x => {
            //         results = df.getColumn(x).toArray().slice(sliceStart, sliceEnd);
            //         console.info(`\t${x}: ${results}`);
            //     });
            //     console.info(`Schema: ${df.schema.fields.map(x => x.name)}`);
            //     console.info(`Voltage Offsets: ${JSON.stringify(voltageOffsets)}`);
            //     console.info(`Casts: ${JSON.stringify(casts)}`);
            // } catch (e) {
            //     logger.error(`Error printing results: ${e}`);
            // }
        }
        end = moment();
        duration = moment.duration(end.diff(hex_file_start)).asSeconds();
        logger.info(`\tProcessing time - item ${idx} - overall file processing: ${duration}s`);
        metrics.push(duration);

        idx += 1;
    }
    let totalTime = metrics.reduce((x, y) => x + y, 0);
    logger.info('Total Processing time');
    logger.info(`\t${idx} items, total time: ${totalTime.toFixed(1)}s, ` +
        `time per item = ${(totalTime/idx).toFixed(1)}s`);
}

async function bulkCalculatePointEstimates() {

    // Bulk Start
    bulkStart = moment();

    // Retrieve the Trawl Survey Haul Data
    logger.info(`Retrieving haul data`)
    start = moment();
    let startYear = '2016';
    let endYear = '2018'
    let masterHauls = await getTrawlSurveyHaulData(startYear, endYear);
    end = moment();
    duration = moment.duration(end.diff(start)).asSeconds();
    logger.info(`\tProcessing time - retrieving haul data: ${duration}s`);

    // Find all of the converted csv files
    logger.info(`Searching for csv files: ${csvDir}`);
    start = moment();
    let csvFilesArray = await getCsvFiles(csvDir);
    writeFileSync(path.join(os.homedir(), "Desktop", "csvFiles.txt"),
        csvFilesArray.join("\n")
    );
    logger.info(`\tcsv file count: ${csvFilesArray.length}`);
    end = moment();
    duration = moment.duration(end.diff(start)).asSeconds();
    logger.info(`\tProcessing time - getting csv files: ${duration}s`);
    
    let thresholdPct: number = 0.05; // Percentage
    let startPct: number = 0.1; // Percentage
    let endPct: number = 0.1; // Percentage
    let rawFile: string = '';
    let dfHauls = {}, haulTable: Table = null, haulsAvgs = [];
    let slicedHaulTimes: Table = null, slicedHaulNumber: Table = null, slicedHaulPcts: Table = null;
    let startIdx: number = null, endIdx: number = null;


    for (let i in csvFilesArray) {

        // if (parseInt(i) === 1) break;

        try {

            logger.info(`Processing ${csvFilesArray[i]}`);
            start = moment();

            // Read the file
            rawFile = readFileSync(csvFilesArray[i], "utf8");

            // Convert the csv file to a arrow table
            let df = await csvToTable(rawFile);

            // Split the table into separate tables based on the haul ID
            let haulsColName: string = "HaulID";
            dfHauls = splitHauls(df, haulsColName);
            
            // Iterate through each of the hauls from the current csv file
            for (let x in dfHauls) {

                try {

                    logger.info(`\thaul = ${x}`);
                    startIdx = dfHauls[x]['startIdx'];
                    endIdx = dfHauls[x]['endIdx'];

                    let masterHaul = masterHauls.filter(col('trawl_id').eq(x));
                    for (let masterHaulDetails of masterHaul){
                        logger.info(`\t\tdw haul details = ${masterHaulDetails}`);

                        let towDateStart = moment(masterHaulDetails["tow_start_timestamp"]);
                        let towDateEnd = moment(masterHaulDetails["tow_start_timestamp"]);
                        let startTime = masterHaulDetails["sampling_start_hhmmss"];
                        let endTime = masterHaulDetails["sampling_end_hhmmss"];
                        
                        startTime = towDateStart.set({'hour': startTime.slice(0,2), 'minute': startTime.slice(2,4), 
                            'second': startTime.slice(4,6)});
                        endTime = towDateEnd.set({'hour': endTime.slice(0,2), 'minute': endTime.slice(2,4), 
                            'second': endTime.slice(4,6)});
                        logger.info(`\t\tdw startTime - sampling = ${startTime.format('HH:mm:ss')}, endTime = ${endTime.format('HH:mm:ss')}`);

                        let slicedHaulNumber = slice(df, startIdx, endIdx);
                        let csvStartTime = moment(slicedHaulNumber.get(0)['DateTime (ISO8601)']);
                        let csvEndTime = moment(slicedHaulNumber.get(slicedHaulNumber.count()-1)['DateTime (ISO8601)']);

                        logger.info(`\t\tHaul Number Slicing`)
                        logger.info(`\t\t\tData size = ${slicedHaulNumber.count()}`);
                        logger.info(`\t\t\tstartIdx = ${startIdx}, endIdx = ${endIdx}`);
                        logger.info(`\t\t\tstartTime = ${csvStartTime.format('MM/DD/YYYY HH:mm:ss')}, endTime = ${csvEndTime.format('MM/DD/YYYY HH:mm:ss')}`)

                        logger.info(`\t\tHaul Start/End Time Slicing`);
                        slicedHaulTimes = sliceByTimeRange(slicedHaulNumber, "DateTime (ISO8601)", startTime, endTime);
                        logger.info(`\t\t\tData size = ${slicedHaulTimes.count()}`);
                        logger.info(`\t\t\tstartTime = ${startTime.format('HH:mm:ss')}, endTime = ${endTime.format('HH:mm:ss')}`);

                        if (slicedHaulTimes.count() > 0) {

                            // Slice the data by droping beginning and ending values by startPct and endPct
                            let startCount: number = null, endCount: number = null;
                            startCount = Math.floor(slicedHaulTimes.count() * startPct);
                            endCount = Math.ceil(slicedHaulTimes.count() * (1 - endPct));
                            slicedHaulPcts = slice(slicedHaulTimes, startCount, endCount);
                            logger.info(`\t\tHaul Percents Slicing`); 
                            logger.info(`\t\t\tData size  = ${slicedHaulPcts.count()}`);
                            // logger.info(`\t\t\tstartTime = ${}, endTime = ${}`);
                            logger.info(`\t\t\tstartCount = ${startCount}, endCount = ${endCount}`)

                            // Auto Remove outliers
                            let averages = await removeOutliers(slicedHaulPcts, thresholdPct);
                            // averages["haulID"] = x;
                            haulsAvgs.push(averages);
                        } else {
                            logger.info(`\t\tNo matching data found, skipping the haul`);
                        }
                    }

                } catch (haulError) {
                    logger.error(`Error processing haul ${x}: ${haulError}`);
                }
 
            }
            end = moment();
            duration = moment.duration(end.diff(start)).asSeconds();
            logger.info(`\tProcessing time: ${duration}s`);

        } catch (fileError) {
            logger.error(`Error processing csv output file ${csvFilesArray[i]}: ${fileError}`)
        }
    }

    // Insert the averages into the Haul Characteristics table
    for (let x in dataStruct) {
        let item = dataStruct[x];
        item["vector"] = haulsAvgs.map((y: any) => {
            return y[item["colName"]];
        })
        // logger.info(`${item["colName"]} vector: ${item["vector"]}`);
    }
    let finalArray = dataStruct.map((x: any) => {
        if (x["vectorType"] === DateVector) {
            return Utf8Vector.from(x["vector"]);
        } else {
            return x["vectorType"].from(x["vector"]);
        }
    });
    let finalCols = dataStruct.map((x: any) => {
        return x["colName"];
    });
    logger.info(`finalCols = ${finalCols}`);
    // logger.info(`finalArray = ${finalArray}`);

    let dfFinal = Table.new(finalArray, finalCols);

    // Save the point estimates out to a final csv file

    let pointEstimatesFile = path.join(pointEstimatesDir, "pointEstimates.csv");
    await saveToFile(dfFinal, "csv", pointEstimatesFile, finalCols);

    bulkEnd = moment();
    duration = moment.duration(bulkEnd.diff(bulkStart)).asSeconds();
    logger.info(`\tBulk Processing time: ${duration} seconds, ${duration/60} minutes, ${duration/3600} hours`);

}

// bulkConvertData();
bulkCalculatePointEstimates();