// import { readFileSync, writeFileSync, writeFile } from 'fs';
import {Table, Null, util} from 'apache-arrow';
import { readFile, writeFile, WorkBook, WorkSheet, utils } from 'xlsx';

const mdFile = "C:\\Users\\Todd.Hay\\Desktop\\md.xlsx";
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

