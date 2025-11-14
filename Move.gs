/**
 * Intended Use:
 * 1. Select files for bucket based reorganization: Create a temporary table 
 *    containing all non-folder, non-shortcut objects in drive using createTableForBucketing()
 *    This table will also serve as our historical snapshot for subsequent reorganization.
 * 2. Note the size of the table and adjust reorgFullTableTize
 * 3. Create a Drive folder with the needed structure, and store the metadata 
 *    for later use with createBucketFolders()
 * 4. Using the temp table from step 2, create new bucketed tables of 400k files each,
 *    paired with the folder ID where they'll need to go generated in step 3, 
 *    using createBucketedTables()
 * 5. Trigger Extracts of the temp tables from step 4 to GCS using extractMoveBuckets()
 * 6. Use GAM to process moving each file to its target folder:
 *    
 *    gam redirect stdout ./logFile.txt multiprocess redirect stderr stdout csv PATH_TO_CSV.csv gam user "~email" move drivefile  "~id" parentid "~target_folder" duplicatefiles uniquename
 *    
 *    This command instructs gam to disambiguate the names of duplicate named files by appending (n) to the file name as it goes:
 * 
 * NOTES ABOUT THE QUERY:
 * This selects unique files in the My Drive corpus with the following deduplication logic:
 *   1. (UniqueChecksumFiles) For each item with duplicate md5_checksums (non-google files), pick the one with the most recent modification timestamp
 *      - most recent is defined as max(last modified time,creation time)
 *      - this logic is here to account for the fact that sometimes last modified time is supplied by the uploading client
 *   2. pick all google file types (docs, sheets, slides, etc), omitting folders and shortcuts
 *      - there's no good way to identify duplication among this cohort from inventory report, so assume they're all unique.
 *   3. Combine the results of 1 and 2 into one table, and apply a row numbering function.
 */



const filesExtractTable = 'inventoryFiles';
const filesBucketTablePrefix = 'inventory_to_move_';
const reorgCsvRowCount = 400000;
const reorgFullTableSize = NUMBER;
// Everyone in the domain whose files are expected to be moved
const usersInDomain = [
  'user@domain.com',
  'user2@domain.com'
]

function takeSnapShot(){
  const job = {
    configuration:{
      query: {
          destinationTable: {
            projectId: projectName,
            datasetId: dataSet,
            tableId: 'drive_snapshot',
          },
          createDisposition: 'CREATE_IF_NEEDED',
          writeDisposition: 'WRITE_TRUNCATE',
          allowLargeResults: true,
          useLegacySql: false,
          query:`SELECT * FROM ${projectName}.${dataSet}.${inventoryTable}`
      }
    }
  }
  Logger.log(job);
  let jobResult = BigQuery.Jobs.insert(job, projectId);
  Logger.log(jobResult.jobReference.jobId)
}

function createTableForBucketing(){
  const job = {
      configuration:{
        query: {
          destinationTable: {
            projectId: projectName,
            datasetId: dataSet,
            tableId: filesExtractTable,
          },
          createDisposition: 'CREATE_IF_NEEDED',
          writeDisposition: 'WRITE_TRUNCATE',
          allowLargeResults: true,
          useLegacySql: false,
          query: `WITH UniqueChecksumFiles AS (
    SELECT
        id,
        owner.user.email as owner_email,
        md5_checksum,
        title,
        size_bytes,
        mime_type,
        parent,
        GREATEST(last_modified_time_micros, create_time_micros) AS max_time,
        ROW_NUMBER() OVER (
            PARTITION BY md5_checksum
            ORDER BY GREATEST(last_modified_time_micros, create_time_micros) DESC
        ) as rn
    FROM
        \`${projectName}.${dataSet}.${inventoryTable}\`
    WHERE
        md5_checksum IS NOT NULL
        AND owner.shared_drive.id is null
), unique_items as (
SELECT
    id,
    owner_email,
    md5_checksum,
    title,
    size_bytes,
    mime_type,
    max_time,
    parent,
FROM
    UniqueChecksumFiles
WHERE
    rn = 1
UNION ALL
SELECT
    id,
    owner.user.email as owner_email,
    md5_checksum, -- This will be NULL, confirming it was the correct group
    title,
    size_bytes,
    mime_type,
    GREATEST(last_modified_time_micros, create_time_micros) AS max_time,
    parent 
FROM
    \`${projectName}.${dataSet}.${inventoryTable}\`
WHERE
    mime_type!='application/vnd.google-apps.folder' 
    AND mime_type != 'application/vnd.google-apps.shortcut'
    AND md5_checksum IS NULL
    AND owner.shared_drive is null
)
SELECT
    ROW_NUMBER() OVER (ORDER BY title asc) as row_number,
    *
FROM
    unique_items
ORDER BY
    title asc`,
        }
      }
    }
  Logger.log(job)
  let jobResult = BigQuery.Jobs.insert(job, projectId);
  Logger.log(jobResult.jobReference.jobId)
}

function createBucketFolders(){
  const reorgRoot = DriveApp.createFolder('Reorganized Files');
  reorgRoot.addEditors(usersInDomain)
  const folderList = []
  for(let i = 0; i < Math.ceil(reorgFullTableSize/reorgCsvRowCount); i+=1){
    const folderName = filesBucketTablePrefix + String(i).padStart(3,'0')
    const folder = reorgRoot.createFolder(folderName)
    folderList.push([folder.getId(),folderName]);
    Logger.log(folder.getId());
  }
  const persistence = reorgRoot.createFile('folderIds.json',JSON.stringify(folderList));
  PropertiesService.getScriptProperties().setProperty('persistenceId',persistence.getId())
}

function createBucketedTables(){
  const persistenceId = PropertiesService.getScriptProperties().getProperty('persistenceId');
  const folderIdsList = JSON.parse(DriveApp.getFileById(persistenceId).getBlob().getDataAsString());
  Logger.log(folderIdsList);
  for(let i = 0; i < Math.ceil(reorgFullTableSize/reorgCsvRowCount); i+=1){
    Logger.log(i)
    const job = {
      configuration:{
        query: {
          destinationTable: {
            projectId: projectName,
            datasetId: dataSet,
            tableId: filesBucketTablePrefix + String(i).padStart(3,'0'),
          },
          createDisposition: 'CREATE_IF_NEEDED',
          writeDisposition: 'WRITE_TRUNCATE',
          allowLargeResults: true,
          useLegacySql: false,
          query: `SELECT '${folderIdsList[i][0]}' as target_folder, * FROM \`${projectName}.${dataSet}.${filesExtractTable}\`
          WHERE row_number BETWEEN ${i*reorgCsvRowCount+1} AND ${(i+1)*reorgCsvRowCount}
          ORDER BY row_number`
        }
      }
    }
    Logger.log(job)
    let jobResult = BigQuery.Jobs.insert(job, projectId);
    Logger.log(jobResult.jobReference.jobId)
  }
}

function extractMoveBuckets(){
  for(let i = 0; i < Math.ceil(reorgFullTableSize/reorgCsvRowCount); i+=1){
    const fileName=`migration_extract_part_${String(i).padStart(3,'0')}.csv`
    const extractJob = {
      configuration: {
        extract: {
          sourceTable: {
            projectId:projectName,
            datasetId:dataSet,
            tableId: filesBucketTablePrefix + String(i).padStart(3,'0')
          },
          destinationUri: gcsBucket+fileName, // Single, full file path in GCS
          destinationFormat: 'CSV',
          printHeader: true, // Include the header row
          fieldDelimiter: ','
        },
      }
    }
    Logger.log(extractJob);
    let jobResult = BigQuery.Jobs.insert(extractJob, projectId);
    Logger.log(jobResult.jobReference.jobId)
  }
}
