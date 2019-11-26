const fs  = require('fs')
const {Storage} = require('@google-cloud/storage')

const storage = new Storage()

/* define public methods */
const download = (job, settings, src, dest, params, type) => {
    const parsed_src = src.replace('gs://', '').split('/')
    const bucket_name = parsed_src[0]
    const item = parsed_src.slice(1).join('/')
    const file = fs.createWriteStream(dest)

    if (!bucket_name) {
        return Promise.reject(new Error('GCS bucket not provided.'))
    }
    if (!item) {
        return Promise.reject(new Error('GCS item not provided.'))
    }

    return new Promise((resolve, reject) => {
        file.on('close', resolve)

        storage
            .bucket(bucket_name)
            .file(item)
            .createReadStream()
            .on('error', reject)
            .pipe(file)
    })
}

const upload = (job, settings, src, params) => {
    if (!params.bucket) {
        return Promise.reject(new Error('GCS bucket not provided.'))
    }
    if (!params.item) {
        return Promise.reject(new Error('GCS item not provided.'))
    }

    const onProgress = (e) => {
        const progress = Math.ceil(e.loaded / e.total * 100)
        settings.logger.log(`[${job.uid}] action-upload: upload progress ${progress}%...`)
    }

    const onComplete = () => {
        settings.logger.log(`[${job.uid}] action-upload: upload complete`)
    }

    return new Promise((resolve, reject) => {
        const bucket = storage.bucket(params.bucket)
        const file = bucket.file(params.item)
        const options = {}
        if (params.contentType) {
            options.metadata = {
                contentType: params.contentType
            }
        }
        // TODO: Set content type
        fs.createReadStream(src)
            .pipe(file.createWriteStream(options))
            .on('error', reject)
            .on('finish', resolve)
            .on('progress', onProgress)
    })
}

module.exports = {
    download,
    upload,
}
