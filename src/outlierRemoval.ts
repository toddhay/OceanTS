import { logger } from "./logger";
import { median, medianThreshold, sum, slice, sliceByTimeRange} from "./structures";
import { Table } from 'apache-arrow';

export async function removeOutliers(data: Table, threshold: number): Promise<Object> {
    /*
    data - arrow table format
    threshold - number representing the percentage from the average for the threholding
    */
    // Throw out outliers by thresholding against the median value
    let avg: number, header: string = "", averages = {}, vec: any, medianVal: number;
    let headers: string[] = data.schema.fields.map((d: any) => d.name);
    for (let i in headers) {
        header = headers[i];
        medianVal = await median(data, header);
        vec = await medianThreshold(data, header, medianVal, threshold);

        // Calculate the averages
        avg = vec.reduce(sum) / vec.length;
        averages[header] = avg;
    }
    return averages;
}