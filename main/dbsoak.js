var // minDate       = new Date(2012, 0, 1, 0, 0, 0, 0),
    // maxDate       = new Date(2013, 0, 1, 0, 0, 0, 0),
    // delta         = maxDate.getTime() - minDate.getTime(),
    documentNumber = arg_max,
    batchSize      = arg_batch,
    doing          = arg_doing,  // inserts replaces upserts
    informPer      = 100000,
    insertCount    = 0,
    updateCount    = 0,
    upsertCount    = 0,
    start          = new Date(),
    batchDocuments = new Array();

if (informPer > documentNumber/10) {
    informPer = Math.round(documentNumber/10)
}

function doBulk() {
    if (batchDocuments.length == 0) { return; }
    if (doing == 'inserts') {
        res = db.js.insert(batchDocuments);
        if (res.writeErrors) { panic(JSON.stringify(res)); }
        insertCount += res.nInserted;
        upsertCount += res.nUpserted;
    } else {
        res = db.js.bulkWrite(batchDocuments, { "ordered" : false });
        if (res.writeErrors) { panic(JSON.stringify(res)); }
        var countIds = 0; for (var i in res.upsertedIds) { countIds++; } res.upsertedIds = countIds;
        // print(JSON.stringify(res));
        insertCount += res.insertedCount;
        upsertCount += res.upsertedCount;
        updateCount += res.modifiedCount;
    }
    batchDocuments = new Array();
}

function informer(docCount) {
    var elapsed = new Date() - start;
    print(new Date().toISOString(), 'js', doing, docCount, '/', documentNumber, '(' + batchSize, 'per batch) in', elapsed/1000.0 + 's =', Math.round((10.0*1000.0*docCount)/elapsed)/10.0 + '/s ins:'+insertCount, 'ups:'+upsertCount, 'mods:'+updateCount);
}

for (var index = 0; index < documentNumber; index++) {
    var myid = index % 1000,
        document = {
            myid: myid,
            created_on: new Date(), // minDate.getTime() + Math.random() * delta),
            value: index // Math.random()
        },
        update_document = {
            '$set': {
                created_on: new Date(),
                value: index
            }
        };
    if (doing == 'inserts') {
        batchDocuments[index] = document;
    } else if (doing == 'replaces') {
        batchDocuments[index] = { "replaceOne": { "filter": { "myid": myid }, "replacement": document, "upsert": true } };
    } else if (doing == 'upserts') {
        batchDocuments[index] = { "updateOne": { "filter": { "myid": myid }, "update": update_document, "upsert": true } };
    }
    if ((index + 1) % batchSize == 0) {
        doBulk()
    }
    if (index % informPer == 0) {
        informer(index)
    }
}
doBulk()
informer(documentNumber)
