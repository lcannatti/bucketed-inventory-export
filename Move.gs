/**
 * Intended Use:
 * 0. Delete the duplicate files found using Deduplicate.gs using GAM/other API 
 *    tool of choice, and wait for a new inventory report to be generated.
 * 1. Create a snapshot of the inventory report with takeSnapShot()
 * 2. Select files for bucket based reorganization: Create an temporary table 
 *    containing all non-folder, non-shortcut objects in drive using createTableForBucketing()
 * 3. Create a Drive folder with the needed folder structure, and store the metadata 
 *    for later use with createBucketFolders()
 * 4. Using the temp table from step 2, create new bucketed tables of 400k files each,
 *    paired with the folder ID where they'll need to go generated in step 3, 
 *    using createBucketedTables()
 * 5. Trigger Extracts of the temp tables from step 4 to GCS using extractMoveBuckets()
 * 6. Use GAM to process moving each file to its target folder:
 *    gam redirect stdout ./logFile.txt multiprocess redirect stderr stdout csv PATH_TO_CSV.csv gam user "~email" move drivefile  "~id" parentid "~target_folder" duplicatefiles uniquename
 */

const filesExtractTable = 'inventoryFiles';
const filesBucketTablePrefix = 'inventory_to_move_';
const reorgCsvRowCount = NUMBER;
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
          query: `SELECT
      ROW_NUMBER() OVER (ORDER BY title ASC) as row_number,
      id,
      if(owner.user.email is not null,owner.user.email,'SHARED DRIVE') as email,
      title,
      size_bytes,
      mime_type,
      md5_checksum,
      last_modified_time_micros,
    FROM
      \`${projectName}.${dataSet}.${inventoryTable}\` as inventory
    WHERE 
      mime_type not in ('application/vnd.google-apps.shortcut', 'application/vnd.google-apps.folder') 
      AND owner.shared_drive.id is null
    ORDER BY title ASC`,
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
