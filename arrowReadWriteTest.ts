import { readFileSync, writeFileSync, writeFile } from 'fs';
import {Table, Null} from 'apache-arrow';
// import axios from 'axios';
import fetch from 'node-fetch';
import * as moment from 'moment';

const dataUrl = "https://raw.githubusercontent.com/RandomFractals/ChicagoCrimes/master/data/2018/chicago-crimes-2018.arrow";
const newFile = "C:\\Users\\Todd.Hay\\Desktop\\data.arrow";

async function retrieveData() {
    try {
        const response = await fetch(dataUrl);
        const dataTable = await response.arrayBuffer().then((buffer: any) => {
            const newData = Table.from(new Uint8Array(buffer));
            writeFile(newFile, new Uint8Array(buffer), (err) => {
                if (err) throw err;
                console.log('The file has been saved!');
            });
            console.log(newData.get(0).toJSON());
            console.log(newData.get(0).toString());
            console.log('rows = ' + newData.count());
            return newData;
            // return Table.from(new Uint8Array(buffer));
        });
        console.log(dataTable.toString());
    }
    catch(err) {
        console.log('Error: ' + err.message);
    }
}

let data = null;
const localFile = true;
if (localFile) {
    let start = moment();
    let data = Table.from(readFileSync(newFile));

    // start = moment();
    console.log('rows = ' + data.count());
    // end = moment();
    // duration = moment.duration(end.diff(start));
    // console.log('time to count: ' + duration.asSeconds() + 's');

    const rowCount = data.count();
    console.log(data.get(0).toJSON());
    console.log(data.get(0).toString());
    console.log(data.get(rowCount-1).toJSON());

    let fields = data.schema.fields.map(f => f.name);
    console.info('fields: ' + fields);

    let end = moment();
    let duration = moment.duration(end.diff(start));
    console.log('time to load: ' + duration.asSeconds() + 's');

} else {
    let data = retrieveData();
    // console.log(data.toString());
}
// console.log('time to load: ' + (end - start));


// let x: number = 10;
// let y: number = 20;
// console.log(x + y);

