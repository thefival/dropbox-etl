const AWS = require('aws-sdk')
const fs = require('fs')
const mysql = require('mysql2/promise')
const path = require('path')
const Dropbox = require('dropbox').Dropbox
const throttledQueue = require('throttled-queue')
const os = require('os')
const path = require('path')

const dbx = new Dropbox({
  accessToken:
    'sl.BdgbLEKnTXD_nYJsr3-gai1ZHslkSaoZ0-RQ4Hxm1BLQJyjTri0k6_82unyoP0jBsBWoAN6w6M8YYSoJ7kaSumGxUDbElv9UbZ1pGtHnUFtc8ZjuJs-bUMt3z39O7pyKcOCpXRJVKJg',
})

process.env.AWS_ACCESS_KEY_ID = 'AKIAYJCGAPMEAFCBAQGZ'
process.env.AWS_SECRET_ACCESS_KEY = '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'

const pool = mysql.createPool({
  connectionLimit: 16,
  host: 'localhost',
  user: 'root',
  password: 'bangarang',
  database: 'absdropbox',
})

const throttledRequestsPerSecond = 15

const throttle = throttledQueue(throttledRequestsPerSecond, 1000)

async function getSKUs() {
  const [rows] = await pool.query('SELECT primary_id FROM pimboe')
  return rows.map((row) => row.primary_id)
}

async function queryDropbox(query) {
  const options = {
    path: '',
    mode: 'filename',
    query: query,
  }
  try {
    const response = await dbx.filesSearchV2(options)
    return response.result.matches ?? []
  } catch (error) {
    console.error(error)
    return []
  }
}

let i = 0

async function processSKU(sku) {
  const query = sku.toString()
  const matches = await queryDropbox(query)
  const skuRegex = new RegExp(`(${sku})`, 'i') // create a regular expression to match the SKU in the filename
  const allowedExtensions = ['.tif']

  if (matches.length === 0) {
    console.log(`No matches found for SKU ${sku}`)
    return
  }
  console.log(`Found ${matches.length} match(es) for SKU ${sku}`)

  let foundAllowedFile = false // flag to keep track of whether at least one file with allowed extension has been found

  for (const match of matches) {
    const { path_display } = match.metadata.metadata
    const fileExtension = path.extname(path_display).toLowerCase()
    const filename = path.basename(path_display)

    if (!allowedExtensions.includes(fileExtension)) {
      console.log("File extension not allowed:", filename)
      continue
    }

    if (!skuRegex.test(filename)) { // check if the filename contains the SKU
      console.log("SKU not found in filename:", filename)
      continue
    }

    // if this point is reached, the file passes the checks
    foundAllowedFile = true // set the flag to true

    const folderName = `${sku}/` // create a folder name based on the SKU
    const s3 = new AWS.S3({ region: 'us-east-2' })

    // check if there's at least one file with an allowed extension
    if (!foundAllowedFile) {
      console.log(`No files with allowed extensions found for SKU ${sku}`)
      return
    }

    // create the folder in S3 if it doesn't exist yet
    try {
      await s3.headObject({ Bucket: 'tif-dropbox', Key: folderName }).promise()
      console.log(`Folder ${folderName} already exists in S3`)
    } catch (error) {
      if (error.statusCode === 404) { // folder doesn't exist yet
        try {
          await s3.putObject({ Bucket: 'tif-dropbox', Key: folderName, Body: '' }).promise()
          console.log(`Folder ${folderName} created in S3`)
        } catch (error) {
          console.error(`Failed to create folder ${folderName} in S3:`, error)
          return
        }
      } else { // unexpected error
        console.error(`Failed to check folder ${folderName} in S3:`, error)
        return
      }
    }

    try {
      let { result: fileData } = await dbx.filesDownload({ path: path_display })
      const fileBuffer = Buffer.from(fileData.fileBinary, 'binary')

      const key = `${folderName}${filename}` // include the folder name in the S3 object key
      const s3Params = {
        Bucket: 'tif-dropbox',
        Key: key,
        Body: fileBuffer,
      }

      const { Location } = await s3.upload(s3Params).promise()
      console.log(`File ${filename} uploaded to S3 at ${Location}`)
      console.log("count", i++)

    } catch (error) {
      console.error(`Failed to upload file ${filename} to S3:`, error)
      continue // This will continue with the next iteration of the loop
    }
  }

  if (!foundAllowedFile) {
    console.log(`No files with allowed extensions found for SKU ${sku}`)
    return
  }
}

async function run() {
  const skus = await getSKUs()
  for (const sku of skus) {
    await throttle(() => processSKU(sku))
  }
}

run()
