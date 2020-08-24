import { Table, DateVector, Float32Vector, Utf8Vector, 
    RecordBatchJSONWriter, RecordBatch, Int32Vector, RecordBatchStreamWriter } from "apache-arrow";
import { col, custom } from 'apache-arrow/compute/predicate';
import * as arrow2csv from 'apache-arrow/bin/arrow2csv';
import Axios from 'axios';
import * as os from 'os';
import * as path from 'path';
import { readFileSync, existsSync, writeFileSync, createWriteStream } from 'fs';
import { readFile } from "xlsx/types";
import * as moment from 'moment-timezone';
import * as fg from 'fast-glob';

import * as csv from 'csvtojson';
import * as csvWriter from 'csv-write-stream';
import * as json2csv from 'json2csv';
import { logger } from './logger';


export function hex2dec(x: string): number {
    return parseInt(x, 16);
}

export function counts2frequency(counts: number): number {
    /* Function to convert Analog/Digital counts to a frequency
        This is used for temperature, pressure, and voltage A/D counts

        counts:  number with 6 digits, each 2 digit group represents a byte of data

        return:  frequency - number
    */

    if (counts.toString().length !== 6) {
        console.error('Counts number of digits is not 6: ' + counts);
        return NaN;
    }
    let countsStr = counts.toString();
    return parseInt(countsStr.slice(0,2)) * 256 +
            parseInt(countsStr.slice(2,4)) +
            parseInt(countsStr.slice(4,6)) / 256

}

export function jsonArray2ArrowTable(jsonData: Object[]): Table {
    let header = Object.keys(jsonData[0]);
    let dataArrays = [];
    let temp: any = null;
    header.forEach(h => {
        temp = jsonData.map(x => x[h]);
        if (['latitude_hi_prec_dd', 'longitude_hi_prec_dd'].includes(h)) {
            dataArrays.push(Float32Vector.from(temp.map((x: any) => parseFloat(x))));
        } else if (['tow_end_timestamp', 'tow_start_timestamp'].includes(h)) { 
            dataArrays.push(DateVector.from(temp.map((x: any) => moment(x, "YYYY-MM-DD HH:mm:ss").toDate())));
        } else {
            dataArrays.push(Utf8Vector.from(temp));
        } 
    })
    let df: Table = Table.new(dataArrays, header);
    return df;
}

export async function getTrawlSurveyHaulData(startYear: string, endYear: string): Promise<Table> {
    /*


    Useful Arrow JS page on json2arrow:
    https://github.com/apache/arrow/blob/master/js/bin/json-to-arrow.js
    */

    // Retrieve Trawl Survey Haul Characteristics data from FRAM Data Warehouse
    let baseUrl = "https://www.nwfsc.noaa.gov/data/api/v1/source/trawl.operation_haul_fact/selection.";
    let selectionType = "csv";  // "json"
    let variables = "latitude_hi_prec_dd,longitude_hi_prec_dd,tow_end_timestamp,tow_start_timestamp,trawl_id,vessel,sampling_start_hhmmss,sampling_end_hhmmss";
    let filters = "year>=" + startYear + ",year<=" + endYear;
    let dwUrl = baseUrl + selectionType + "?" + "filters=" + filters + "&" + "variables=" + variables;
    // console.info(`\tdwUrl = ${dwUrl}`);

    const desktopDir = path.join(os.homedir(), "Desktop");
    const haulsArrowFile = path.join(desktopDir, "hauls.arrow");
    const haulsFile = path.join(desktopDir, "hauls." + selectionType);
    try {
        let data: any;
        if (!existsSync(haulsFile)) {
            const response = await Axios.get(dwUrl);
            data = response.data;
            if (selectionType === "json") {
                writeFileSync(haulsFile, JSON.stringify(data));
            } else {
                writeFileSync(haulsFile, data);
                data = await csv().fromString(data);    // Convert csv to an array of JSON objects
            }
            logger.info(`\tdata retrieved successfully`);
        } else {
            if (selectionType === "csv") {
                data = await csv().fromFile(haulsFile);   // Convert csv to an array of JSON objects
            } else {
                data = readFileSync(haulsFile);
            }
        }
        logger.info(`\thaul data successfully opened ...`);

        let df = jsonArray2ArrowTable(data);
        logger.info(`\thaul data successfully converted to an arrow table`);

        return df;
    } catch (e) {
        logger.error(`\tError in retrieving trawl survey haul data: ${e}`);
    }
    return null;
}

export async function getHexFiles(dataDir: string): Promise<Array<string>> {
    let hexFiles: string[] = [];
    hexFiles = fg.sync([dataDir + '/**/*.hex'], 
        {nocase: true, ignore: [
            dataDir + '/**/*test*.hex',
            dataDir + '/**/*calibration*.hex',
            dataDir + '/**/OrangeBoatCastTemplates/**/*.hex',
            dataDir + '/**/BlueBoatCastTemplates/**/*.hex',
    ]});
    hexFiles = hexFiles.toString().split(",");
    return hexFiles;
}

export async function getCsvFiles(dataDir: string): Promise<Array<string>> {
    let csvFiles: string[] = [];
    csvFiles = fg.sync([dataDir + '/**/*.csv'], {nocase: true});
    csvFiles = csvFiles.toString().split(",");
    return csvFiles;
}

export async function getXmlconFiles(dataDir: string): Promise<Array<string>> {
    let xmlconFiles: string[] = [];
    xmlconFiles = fg.sync([dataDir + '/[0-9]{4}/[0-9]{4}_CTD_ConFiles_Raw/SBE19plusV2_*.xmlcon'], 
        {nocase: true, ignore: [
            dataDir + '/**/*_CTD_Leg\d_*/*.xmlcon',
    ]});
    xmlconFiles = xmlconFiles.toString().split(",");
    return xmlconFiles;
}

export async function mergeLatitudeIntoCasts(hauls: Table, casts: Object[], 
                                             vessel: string,
                                             scanRate: number): Promise<Array<Object>> {
    
    // VesselMap is required for translating from vessel folder path to named pulled from the data warehouse
    let vesselMap = {
        "Excalibur": "Excalibur",
        "LastStraw": "Last Straw",
        "MsJulie": "Ms. Julie",
        "NoahsArk": "Noahs Ark"
    };
    
    if (hauls !== null) {
        let castStart: Date = null, castEnd: Date = null;
        let haulID: any = null, lat: any = null, lon: any = null;
        casts.forEach(x => {
            x["startDate"] = moment(x["startDate"]).tz("America/Los_Angeles").format();
            x["endDate"]  = moment(x["startDate"]).add((x["endNum"] - x["startNum"]) / scanRate, 'seconds').tz("America/Los_Angeles").format();
            const haulsDateFilter = custom(i => {
                let haulStart = moment(hauls.getColumn("tow_start_timestamp").get(i)).tz("America/Los_Angeles").format();
                let haulEnd = moment(hauls.getColumn("tow_end_timestamp").get(i)).tz("America/Los_Angeles").format();
                return haulStart < x["endDate"] && haulEnd > x["startDate"];
            }, b => 1);

            hauls.filter(haulsDateFilter.and(col("vessel").eq(vesselMap[vessel])))
                .scan((idx) => {
                    x["latitude"] = lat(idx);
                    x["longitude"] = lon(idx);
                    x["haulID"] = haulID(idx);
                }, (batch) => {
                    lat = col('latitude_hi_prec_dd').bind(batch);
                    lon = col('longitude_hi_prec_dd').bind(batch);
                    haulID = col('trawl_id').bind(batch);
                });
        });
    }  
    return casts;
}

export async function addHaulInfoToTable(df: Table, casts: Object[], scanRate: number = 4): Promise<Table> {
    /* Function to add haul ID, latitude, longitude, date/time columns to the table
        df - arrow table
        casts - array of cast objects that contain the haul information
    */    
    let latitude = new Float32Array(df.length);
    let longitude = new Float32Array(df.length);
    let haulID = new Array(df.length);
    let isoDateTime = new Array(df.length), year = new Array(df.length), month = new Array(df.length), 
        day = new Array(df.length);
    let filteredCasts: Object[] = null;
    let currentDateTime: moment.Moment = null;
    let secondsToAdd: number = -1;

    df.scan((idx) => {
        filteredCasts = casts.filter(x => {
            return idx >= x['startNum'] - 1 && idx < x['endNum'];
        })
        if (filteredCasts.length === 1) {
            if ("latitude" in filteredCasts[0]) {
                latitude[idx] = filteredCasts[0]["latitude"];
                longitude[idx] = filteredCasts[0]["longitude"];
                haulID[idx] = filteredCasts[0]["haulID"];
            }
            if ("startDate" in filteredCasts[0] && "startNum" in filteredCasts[0]) {
                if (idx >= filteredCasts[0]["startNum"] - 1) {
                    secondsToAdd = (idx - filteredCasts[0]["startNum"])/scanRate;   
                    currentDateTime = moment(filteredCasts[0]["startDate"])
                        .add(secondsToAdd, "seconds").tz("America/Los_Angeles");

                    // Testing purposes, output some intermediary values to see if they are correct or not
                    // if (idx % 1000 === 0) {
                    //     logger.info(`\t\tidx = ${idx}, secondsToAdd = ${secondsToAdd}, startDate = ${filteredCasts[0]["startDate"]}, currentTime = ${currentDateTime.format()}`);
                    // }
                    isoDateTime[idx] = currentDateTime.format();
                    year[idx] = currentDateTime.format("YYYY");
                    month[idx] = currentDateTime.format("MM");
                    day[idx] = currentDateTime.format("DD");
                }
            }
        }
    }, (batch) => {} );
    let newCols = ["Latitude (decDeg)", "Longitude (decDeg)", "HaulID", "DateTime (ISO8601)", "Year", "Month", "Day"];
    df = df.assign(Table.new([Float32Vector.from(latitude), 
                              Float32Vector.from(longitude),
                              Utf8Vector.from(haulID), 
                              Utf8Vector.from(isoDateTime),
                              Utf8Vector.from(year),
                              Utf8Vector.from(month),
                              Utf8Vector.from(day)
                             ], newCols));
    return df;
}

export async function saveToFile(df: Table, format: string = "csv", filename: string,
                                 outputColumns: string[]) {
    /* Function to save an arrow table down to disk

    possible formats include:  csv, xlsx, arrow

    */
    let acceptableFormats = ["csv", "xlsx", "arrow"];
    if (!acceptableFormats.includes(format)) {
        console.info(`Save format is not supported: ${format}, not saving to disk`);
        return;
    } 

    // Specify only the columns of interest
    let dfCols = [];
    outputColumns.forEach(x => {
        dfCols.push(df.getColumn(x));
    })
    df = Table.new(dfCols, outputColumns);
    let header = df.schema.fields.map(x => x.name);

    // TESTING
    // format = "arrow";
    // filename = filename.slice(0, -3) + "arrow";

    let writeStream = createWriteStream(filename);
    if (format === "csv") {

        // await RecordBatchJSONWriter.writeAll(df).pipe(writeStream);
        // let parser = json2csv.parse(RecordBatchJSONWriter.writeAll(df));

        let writer = csvWriter({"headers": header});
        writer.pipe(writeStream);
        for (let i=0; i<df.length; i++) {
            await writer.write(df.get(i).toJSON());
        }
        writer.end();

    } else if (format === "xlsx") {

    } else if (format === "arrow") {

        await RecordBatchStreamWriter.writeAll(df).then(x => {
            x.pipeTo(writeStream)
        }).pipeTo(writeStream);

        const arrow = readFileSync(filename);
        const table = Table.from([arrow]);
        let row = table.get(0);
        console.log(`row = ${row}`);

    }
}