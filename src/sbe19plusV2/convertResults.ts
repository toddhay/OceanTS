import { Table } from 'apache-arrow';
import { col, custom } from 'apache-arrow/compute/predicate';
import { pressure } from './equations/pressure';
import { temperature } from './equations/temperature';
import { conductivity } from './equations/conductivity';
import { salinity } from './equations/salinity';
import { oxygen_sbe43, oxygen_optode } from './equations/oxygen';
import { depth} from './equations/depth';
import { turbidity } from './equations/turbidity';
import { fluorescence } from './equations/fluorescence';
import * as moment from 'moment';


export async function convertToEngineeringUnits (instrument: Object, coefficients: Object[], casts: Object[], 
                                                 voltageOffsets: Object, pumpDelay: number, df: Table,
                                                 hauls?: Table) {
    /*
    Function to convert the raw, decimal data parsed from the hex file over to engineering units.
    The data has already been converted to decimal units from hexadecimal units in the df input.  This
    function relies heavily upon the equations found in the equations folder for the instrument-specific 
    conversion equations
    */
    console.info(`Converting to Engineering Units`);
    let start = moment();

    // Scan Rate - use for the temporal + spatial data integration
    let scanRate = 4;

    // Temperature (degC)
    df = await temperature(df, "Temperature A/D Counts", coefficients[0]['TemperatureSensor']);

    // Pressure (dbars)
    df = await pressure(df, "Pressure A/D Counts", "Pressure Temperature Compensation Voltage", coefficients[2]["PressureSensor"]);

    // Conductivity (S_per_m)
    df = await conductivity(df, "Conductivity Frequency", coefficients[1]["ConductivitySensor"]);

    // Salinity (psu)
    df = await salinity(df);

    // Oxygen, SBE 43 (ml_per_l)

    // ToDo Add Voltage 0 offset/slope
    df = await oxygen_sbe43(df, "External Voltage 0", coefficients[3]["OxygenSensor"], scanRate);

    // Fluorometer - "External Voltage 2"
    // ToDo Add Voltage 0 offset/slope
    
    // Turbidity - "External Voltage 3"
    // ToDo Add Voltage 0 offset/slope
    
    // Oxygen Optode, Aanderaa
    df = await oxygen_optode(df, "OPTODE Oxygen", coefficients[6]["OptodeOxygenAanderaa"]);

    let end = moment();
    let duration = moment.duration(end.diff(start)).asSeconds();
    console.info(`\tProcessing time - converting to engineering units: ${duration}s`);


    // Add Haul, Date/Time, Latitude, Longitude data to the arrow Table from the data warehouse
    console.info(`Matching haul latitude/longitude data to cast`);
    start = moment();
    if (hauls !== null) {
        let dfSlice: Table = null, castStart: Date = null, haulsFound: any = null, castEnd: Date = null;
        let castVessel: string = "Excalibur";
        let castLat: number = null, castLon: number = null, castHaul: string = "";
        let haulID: any = null, lat: any = null, lon: any = null, towStart: any = null, towEnd: any = null, vessel: any = null;
        let results = [];
        casts.forEach(x => {
            dfSlice = df.slice(x["startNum"] - 1, x["endNum"] - 1);
            castStart = x["startDate"];
            castEnd = moment(castStart).add((x["endNum"] - x["startNum"]) / scanRate, 'seconds').toDate();

            // TODO - Fix finding the haul that matches the startDateTime of the current cast, this is not working
            // let filter1: any = null, filter2: any = null;
            // filter1 = col("tow_start_timestamp").le(castStart).and(col("tow_end_timestamp").ge(castStart));
            // filter2 = col("tow_start_timestamp").le(castEnd).and(col("tow_end_timestamp").ge(castEnd));

            const haulsDateFilter = custom(i => {
                let haulStart = hauls.getColumn("tow_start_timestamp").get(i);
                let haulEnd = hauls.getColumn("tow_end_timestamp").get(i);
                return haulStart < castEnd && haulEnd > castStart;
            }, b => 1);

            haulsFound = hauls.filter(haulsDateFilter.and(col("vessel").eq(castVessel)))
                .scan((idx) => {
                    castLat = lat(idx);
                    castLon = lon(idx);
                    castHaul = haulID(idx);
                    // results.push({
                    //     'lat': lat(idx),
                    //     'lon': lon(idx),
                    //     'vessel': vessel(idx),
                    //     'haulID': haulID(idx),
                    //     'haulStart': towStart(idx),
                    //     'haulEnd': towEnd(idx)
                    // });
                }, (batch) => {
                    lat = col('latitude_hi_prec_dd').bind(batch);
                    lon = col('longitude_hi_prec_dd').bind(batch);
                    haulID = col('trawl_id').bind(batch);
                    // vessel = col('vessel').bind(batch);
                    // towStart = col('tow_start_timestamp').bind(batch);
                    // towEnd = col('tow_end_timestamp').bind(batch);
                });
            console.info(`\tcast=${x['cast']}, castStart = ${castStart},  castEnd = ${castEnd},  sample count: ${x['endNum'] - x['startNum']}`);
            console.info(`\t\thaulID = ${castHaul}, lat = ${castLat}, lon = ${castLon}`);
        });
    }
    end = moment();
    duration = moment.duration(end.diff(start)).asSeconds();
    console.info(`\tProcessing time - matching haul lat/lons to casts: ${duration}s`);


    // Depth - Requires Latitude data first


    // Display the results
    let msgArray = ["Temperature (degC)", "Pressure (dbars)", "Conductivity (S_per_m)",
        "Salinity (psu)", "Oxygen (ml_per_l)", "OPTODE Oxygen (ml_per_l)"];
    let results = [];
    let sliceStart: number = 30, sliceEnd: number = 35;

    // console.info("Calibration Coefficients");
    // coefficients.forEach(x => { console.info(`\tcoeff: ${JSON.stringify(x)}`); });
    // console.info(`Elements ${sliceStart} to ${sliceEnd-1} of the columns:`)
    msgArray.forEach(x => {
        results = df.getColumn(x).toArray().slice(sliceStart, sliceEnd);
        // console.info(`\t${x}: ${results}`);
    });
    // console.info(`Schema: ${df.schema.fields.map(x => x.name)}`);
    // console.info(`Voltage Offsets: ${JSON.stringify(voltageOffsets)}`);
    // console.info(`Casts: ${JSON.stringify(casts)}`);
}