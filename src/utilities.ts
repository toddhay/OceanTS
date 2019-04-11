import { Table, DateVector, Float32Vector, Utf8Vector } from "apache-arrow";
import { col, custom } from 'apache-arrow/compute/predicate';
import Axios from 'axios';
import * as os from 'os';
import * as path from 'path';
import { readFileSync, existsSync, writeFileSync, readdirSync } from 'fs';
import { readFile } from "xlsx/types";
import * as csv from 'csvtojson';
import * as moment from 'moment';
import * as glob from 'glob';
import * as fg from 'fast-glob';

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

export async function getTrawlSurveyHaulData(): Promise<Table> {
    /*


    Useful Arrow JS page on json2arrow:
    https://github.com/apache/arrow/blob/master/js/bin/json-to-arrow.js
    */

    // Retrieve Trawl Survey Haul Characteristics data from FRAM Data Warehouse
    let baseUrl = "https://www.nwfsc.noaa.gov/data/api/v1/source/trawl.operation_haul_fact/selection.";
    let selectionType = "csv";  // "json"
    let variables = "latitude_hi_prec_dd,longitude_hi_prec_dd,tow_end_timestamp,tow_start_timestamp,trawl_id,vessel";
    let filters = "year>=2016,year<=2018";
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
            console.info(`\tdata retrieved successfully`);
        } else {
            if (selectionType === "csv") {
                data = await csv().fromFile(haulsFile);   // Convert csv to an array of JSON objects
            } else {
                data = readFileSync(haulsFile);
            }
        }
        console.info(`\thaul data successfully opened ...`);

        let df = jsonArray2ArrowTable(data);
        console.info(`\thaul data successfully converted to an arrow table`);

        return df;
    } catch (e) {
        console.error(`\tError in retrieving trawl survey haul data: ${e}`);
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
    return hexFiles;
}

export async function getXmlconFiles(dataDir: string): Promise<Array<string>> {
    let xmlconFiles: string[] = [];
    xmlconFiles = fg.sync([dataDir + '/[0-9]{4}/[0-9]{4}_CTD_ConFiles_Raw/SBE19plusV2_*.xmlcon'], 
        {nocase: true, ignore: [
            dataDir + '/**/*_CTD_Leg\d_*/*.xmlcon',
    ]});
    return xmlconFiles;
}

export async function mergeLatitudeIntoCasts(hauls: Table, casts: Object[], 
                                             vessel: string,
                                             scanRate: number): Promise<Array<Object>> {
    if (hauls !== null) {
        let castStart: Date = null, castEnd: Date = null;
        let haulID: any = null, lat: any = null, lon: any = null;
        casts.forEach(x => {
            castStart = x["startDate"];
            castEnd = moment(castStart).add((x["endNum"] - x["startNum"]) / scanRate, 'seconds').toDate();
            const haulsDateFilter = custom(i => {
                let haulStart = hauls.getColumn("tow_start_timestamp").get(i);
                let haulEnd = hauls.getColumn("tow_end_timestamp").get(i);
                return haulStart < castEnd && haulEnd > castStart;
            }, b => 1);

            hauls.filter(haulsDateFilter.and(col("vessel").eq(vessel)))
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
    let dateTime = new Array(df.length);
    let filteredCasts: Object[] = null;
    let timeShift: number = null;

    df.scan((idx) => {
        filteredCasts = casts.filter(x => {
            return idx >= x['startNum'] - 1 && idx < x['endNum'];
        })
        latitude[idx] = filteredCasts[0]["latitude"];
        longitude[idx] = filteredCasts[0]["longitude"];
        haulID[idx] = filteredCasts[0]["haulID"];
        dateTime[idx] = moment(filteredCasts[0]["startDate"]).add(idx/scanRate, "seconds")
            .format("YYYYMMDD HH:mm:ss.SSS");      
    }, (batch) => {} );
    let newCols = ["Latitude (decDeg)", "Longitude (decDeg)", "HaulID", "DateTime"];
    df = df.assign(Table.new([Float32Vector.from(latitude), 
                              Float32Vector.from(longitude),
                              Utf8Vector.from(haulID), 
                              Utf8Vector.from(dateTime)], newCols));
    return df;
}

export async function saveToFile(df: Table, format: string = "csv", filename: string) {
    /* Function to save an arrow table down to disk

    possible formats include:  csv, xlsx, arrow

    */
    let formats = ["csv", "xlsx", "arrow"];
    if (!formats.includes(format)) {
        console.info(`Save format is not supported: ${format}, not saving to disk`);
        return;
    } 



}