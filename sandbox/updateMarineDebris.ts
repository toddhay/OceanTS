// import { readFileSync, writeFileSync, writeFile } from 'fs';
import {Table, Null, util} from 'apache-arrow';
import { readFile, writeFile, WorkBook, WorkSheet, utils } from 'xlsx';
import * as path from 'path';
import * as os from 'os';
import { existsSync, writeFileSync, readFileSync } from 'fs';
import Axios from 'axios';
import * as csv from 'csvtojson';

export async function getTrawlSurveyHaulData(): Promise<any> {
    /*


    Useful Arrow JS page on json2arrow:
    https://github.com/apache/arrow/blob/master/js/bin/json-to-arrow.js
    */

    // Retrieve Trawl Survey Haul Characteristics data from FRAM Data Warehouse
    let baseUrl = "https://www.nwfsc.noaa.gov/data/api/v1/source/trawl.operation_haul_fact/selection.";
    let selectionType = "csv";  // "json"
    let variables = "latitude_hi_prec_dd,longitude_hi_prec_dd,tow_end_timestamp,tow_start_timestamp,trawl_id,vessel," +
        "gear_depth_m_der,area_swept_ha_der,net_height_m_der";
    let filters = "year>=2016,year<=2018";
    let dwUrl = baseUrl + selectionType + "?" + "filters=" + filters + "&" + "variables=" + variables;
    // console.info(`\tdwUrl = ${dwUrl}`);

    const desktopDir = path.join(os.homedir(), "Desktop");
    const haulsFile = path.join(desktopDir, "hauls." + selectionType);
    try {
        let data: any;
        if (!existsSync(haulsFile)) {
            const response = await Axios.get(dwUrl);
            console.info(`response = ${response}`);
            data = response.data;
            if (selectionType === "json") {
                writeFileSync(haulsFile, JSON.stringify(data));
            } else {
                writeFileSync(haulsFile, data);
                data = await csv().fromString(data);    // Convert csv to an array of JSON objects
            }
        } else {
            console.info(`found it`);
            if (selectionType === "csv") {
                console.info('before csv read');
                data = await csv().fromFile(haulsFile);   // Convert csv to an array of JSON objects
                console.info('after csv');
            } else {
                console.info('before json read');
                data = readFileSync(haulsFile);
            }
        }
        return data;
    } catch (e) {
        console.error(`error encounted: ${e}`)
    }
    return null;
}


async function processData() {
    console.info(`before retrieving trawl data`);
    let data = await getTrawlSurveyHaulData();
    console.info(`data = ${data}`);
}

processData();

process.exit(0);


const mdFile = "C:\\Users\\Todd.Hay\\Desktop\\MarineDebri.xlsx";
// const mdFile = "C:\\Users\\Todd.Hay\\Desktop\\md.xlsx";
const mdNewFile = "C:\\Users\\Todd.Hay\\Desktop\\mdNew.xlsx";
const trawlFile = "C:\\Users\\Todd.Hay\\Desktop\\selection.xlsx";

const wb: WorkBook = readFile(mdFile);
const ws: WorkSheet = wb.Sheets[wb.SheetNames[0]];
let mdJson = utils.sheet_to_json(ws);

const trawlWb: WorkBook = readFile(trawlFile);
const trawlWs: WorkSheet = trawlWb.Sheets[trawlWb.SheetNames[0]];
let trawlJson = utils.sheet_to_json(trawlWs);
let idx: number;
console.info('trawl: ' + JSON.stringify(mdJson));

mdJson.forEach(item => {

    if (!("Latitude_dd" in item) ||
        !("Longitude_dd" in item)) {
        idx = trawlJson.findIndex(x => {
            if (x["trawl_id"] === item["Haul_ID"]) {
                return true
            }
            return false;        
        })
        if (idx !== -1) {
            console.info('trawl found: ' + JSON.stringify(trawlJson[idx]));
            item['Latitude_dd'] = trawlJson[idx]['latitude_dd'];
            item['Longitude_dd'] = trawlJson[idx]['longitude_dd'];
            console.info('md: ' + JSON.stringify(item));

        }
    }
});

let newMd = utils.json_to_sheet(mdJson);
let newWb = utils.book_new();
utils.book_append_sheet(newWb, newMd, 'Marine Debris')

// writeFile(newWb, mdNewFile);


// throw "stop execution";

