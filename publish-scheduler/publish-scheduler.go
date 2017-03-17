package main

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/ONSdigital/dp-publish-pipeline/kafka"
	"github.com/ONSdigital/dp-publish-pipeline/utils"

	"database/sql"

	_ "github.com/lib/pq"
)

var (
	tick             = time.Millisecond * 330
	verboseTick      = false
	maxLaunchPerTick = 20
)

type dbMetaObj struct {
	db      *sql.DB
	prepped map[string]*sql.Stmt
}

type scheduleJob struct {
	scheduleId     int64
	collectionId   string
	collectionPath string
	encryptionKey  string
	scheduleTime   int64
	files          []fileObj
	urisToDelete   []fileObj
}

type fileObj struct {
	filePath string
	fileId   int64
}

func findCollectionFiles(zebedeeRoot, collectionPath string) ([]fileObj, error) {
	var files []fileObj
	searchPath := filepath.Join(zebedeeRoot, "collections", collectionPath, "complete")
	filepath.Walk(searchPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Panicf("Walk failed: %s", err.Error())
		}
		relPath, relErr := filepath.Rel(searchPath, path)
		if !info.IsDir() && relErr == nil {
			files = append(files, fileObj{filePath: relPath})
		}
		return nil
	})
	return files, nil
}

func publishCollection(job scheduleJob, fileProducerChannel, deleteProducerChannel, totalProducerChannel chan []byte) {
	if job.collectionId == "" {
		log.Panicf("Bad message: %v", job)
	}

	var data []byte
	var err error

	// Send published files to the kafka topic
	for i := 0; i < len(job.files); i++ {
		if data, err = json.Marshal(kafka.PublishFileMessage{ScheduleId: job.scheduleId, FileId: job.files[i].fileId, CollectionId: job.collectionId, CollectionPath: job.collectionPath, EncryptionKey: job.encryptionKey, FileLocation: job.files[i].filePath}); err != nil {
			log.Panic(err)
		}
		fileProducerChannel <- data
	}
	log.Printf("Job %d Collection %q sent %d files", job.scheduleId, job.collectionId, len(job.files))

	// Send published deletes to the kafka topic
	for i := 0; i < len(job.urisToDelete); i++ {
		if data, err = json.Marshal(kafka.PublishDeleteMessage{ScheduleId: job.scheduleId,
			DeleteId:       job.urisToDelete[i].fileId,
			DeleteLocation: job.urisToDelete[i].filePath,
			CollectionId:   job.collectionId}); err != nil {
			log.Panic(err)
		}
		deleteProducerChannel <- data
	}
	log.Printf("Job %d Collection %q sent %d deletes", job.scheduleId, job.collectionId, len(job.urisToDelete))
}

func scheduleCollection(jsonMessage []byte, zebedeeRoot string, dbMeta dbMetaObj) {
	var message kafka.ScheduleMessage
	if err := json.Unmarshal(jsonMessage, &message); err != nil {
		log.Panicf("Failed to parse json: %v", jsonMessage)
	} else if len(message.CollectionId) == 0 {
		log.Panicf("Empty collectionId: %v", jsonMessage)
	} else {
		scheduleTime, err := strconv.ParseInt(message.ScheduleTime, 10, 64)
		if err != nil {
			log.Panicf("Collection %q Cannot numeric convert: %q", message.CollectionId, message.ScheduleTime)
		}
		scheduleTime *= 1000 * 1000 * 1000 // convert from epoch (seconds) to epoch-nanoseconds (UnixNano)
		newJob := scheduleJob{collectionId: message.CollectionId,
			collectionPath: message.CollectionPath,
			scheduleTime:   scheduleTime,
			encryptionKey:  message.EncryptionKey}

		var files []fileObj
		if files, err = findCollectionFiles(zebedeeRoot, message.CollectionPath); err != nil {
			log.Panic(err)
		}
		newJob.files = make([]fileObj, len(files))
		copy(newJob.files, files)

		log.Printf("%+v", message.UrisToDelete)
		var deletes []fileObj
		for i := 0; i < len(message.UrisToDelete); i++ {
			log.Println(message.UrisToDelete[i])
			deletes = append(deletes, fileObj{filePath: message.UrisToDelete[i]})
		}
		log.Printf("%+v", deletes)
		newJob.urisToDelete = make([]fileObj, len(deletes))
		copy(newJob.urisToDelete, deletes)
		newJob.scheduleId = storeJob(dbMeta, &newJob)
		log.Printf("Job %d Collection %q scheduled %d files with %d deletes", newJob.scheduleId, newJob.collectionId, len(newJob.files), len(newJob.urisToDelete))
	}
}

func checkSchedule(publishChannel chan scheduleJob, dbMeta dbMetaObj, restartGapNano int64) {
	epochTime := time.Now().UnixNano()
	launchedThisTick := 0

	var rows *sql.Rows
	var err error
	if restartGapNano == 0 {
		rows, err = dbMeta.prepped["select-ready"].Query(epochTime)
	} else {
		rows, err = dbMeta.prepped["select-ready"].Query(epochTime, epochTime-restartGapNano)
	}
	if err != nil {
		log.Panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			collectionId, collectionPath, encryptionKey sql.NullString
			scheduleId, startTime, scheduleTime         sql.NullInt64
		)

		if err = rows.Scan(&scheduleId, &startTime, &scheduleTime, &collectionId, &collectionPath, &encryptionKey); err != nil {
			log.Panic(err)
		}

		if maxLaunchPerTick > 0 && launchedThisTick >= maxLaunchPerTick {
			log.Printf("Job %d Collection %q skip busy - this-tick: %d/%d", scheduleId.Int64, collectionId.String, launchedThisTick, maxLaunchPerTick)
			continue
		}
		files := loadDataFromDataBase(dbMeta, scheduleId.Int64, false)
		deletes := loadDataFromDataBase(dbMeta, scheduleId.Int64, true)
		log.Printf("delets : %d", len(deletes))
		jobToGo := scheduleJob{scheduleId: scheduleId.Int64,
			collectionId:   collectionId.String,
			collectionPath: collectionPath.String,
			scheduleTime:   scheduleTime.Int64,
			encryptionKey:  encryptionKey.String,
			files:          files,
			urisToDelete:   deletes}
		publishChannel <- jobToGo
		launchedThisTick++
		log.Printf("Job %d Collection %q launch#%d with %d files at time:%d", scheduleId.Int64, collectionId.String, launchedThisTick, len(files), epochTime)
	}
	if verboseTick && launchedThisTick == 0 {
		log.Printf("No collections ready at %d", epochTime)
	}
}

func loadDataFromDataBase(dbMeta dbMetaObj, scheduleId int64, loadDeletes bool) []fileObj {
	sqlStmt := "load-incomplete-files"
	if loadDeletes {
		sqlStmt = "load-incomplete-deletes"
	}
	rows, err := dbMeta.prepped[sqlStmt].Query(scheduleId)
	if err != nil {
		log.Panic(err)
	}
	defer rows.Close()

	var files []fileObj
	for rows.Next() {
		var (
			fileId   sql.NullInt64
			filePath sql.NullString
		)
		if err = rows.Scan(&fileId, &filePath); err != nil {
			log.Panic(err)
		}

		files = append(files, fileObj{fileId: fileId.Int64, filePath: filePath.String})
	}
	if err = rows.Err(); err != nil {
		log.Panic(err)
	}
	if loadDeletes {
		log.Printf("Job %d Loaded %d deletes", scheduleId, len(files))
	} else {
		log.Printf("Job %d Loaded %d files", scheduleId, len(files))
	}
	return files
}

func storeJob(dbMeta dbMetaObj, jobObj *scheduleJob) int64 {
	var (
		txn                           *sql.Tx
		fileStmt, jobStmt, deleteStmt *sql.Stmt
		err                           error
		scheduleId                    sql.NullInt64
		fileId                        int64
	)

	if txn, err = dbMeta.db.Begin(); err != nil {
		log.Panic(err)
	}

	if jobStmt, err = txn.Prepare("INSERT INTO schedule (collection_id, collection_path, schedule_time, encryption_key, start_time, complete_time) VALUES ($1, $2, $3, $4, NULL, NULL) RETURNING schedule_id"); err != nil {
		txn.Rollback()
		log.Panic(err)
	}
	if fileStmt, err = txn.Prepare("INSERT INTO schedule_file (schedule_id, file_path) VALUES ($1, $2) RETURNING schedule_file_id"); err != nil {
		txn.Rollback()
		log.Panic(err)
	}
	if deleteStmt, err = txn.Prepare("INSERT INTO schedule_delete (schedule_id, delete_path) VALUES ($1, $2) RETURNING schedule_delete_id"); err != nil {
		txn.Rollback()
		log.Panic(err)
	}

	// insert job into schedule
	res := jobStmt.QueryRow((*jobObj).collectionId, (*jobObj).collectionPath, (*jobObj).scheduleTime, (*jobObj).encryptionKey)
	if err = res.Scan(&scheduleId); err != nil {
		txn.Rollback()
		log.Panic(err)
	}
	// insert files into schedule_file
	for i := 0; i < len((*jobObj).files); i++ {
		if fileId, err = storeFile(fileStmt, scheduleId.Int64, (*jobObj).files[i].filePath); err != nil {
			txn.Rollback()
			log.Panic(err)
		}
		(*jobObj).files[i].fileId = fileId
	}

	// insert files into schedule_file
	for i := 0; i < len((*jobObj).urisToDelete); i++ {
		if _, err = storeFile(deleteStmt, scheduleId.Int64, (*jobObj).urisToDelete[i].filePath); err != nil {
			txn.Rollback()
			log.Panic(err)
		}
		(*jobObj).urisToDelete[i].fileId = fileId
	}

	if err = txn.Commit(); err != nil {
		txn.Rollback()
		log.Panic(err)
	}

	return scheduleId.Int64
}

func storeFile(stmt *sql.Stmt, scheduleId int64, filePath string) (int64, error) {
	res := stmt.QueryRow(scheduleId, filePath)
	var fileId sql.NullInt64
	if err := res.Scan(&fileId); err != nil {
		return 0, err
	}
	return fileId.Int64, nil
}

func (dbMeta dbMetaObj) prep(tag, sql string) {
	var err error
	dbMeta.prepped[tag], err = dbMeta.db.Prepare(sql)
	if err != nil {
		log.Panicf("Error: Could not prepare %q statement on database: %s", tag, err.Error())
	}
}

func main() {
	zebedeeRoot := utils.GetEnvironmentVariable("ZEBEDEE_ROOT", "../test-data/")
	if fileinfo, err := os.Stat(zebedeeRoot); err != nil || fileinfo.IsDir() == false {
		log.Panicf("Cannot see directory %q", zebedeeRoot)
	}

	scheduleTopic := utils.GetEnvironmentVariable("SCHEDULE_TOPIC", "uk.gov.ons.dp.web.schedule")
	produceFileTopic := utils.GetEnvironmentVariable("PUBLISH_FILE_TOPIC", "uk.gov.ons.dp.web.publish-file")
	produceDeleteTopic := utils.GetEnvironmentVariable("PUBLISH_DELETE_TOPIC", "uk.gov.ons.dp.web.publish-delete")
	produceTotalTopic := utils.GetEnvironmentVariable("PUBLISH_COUNT_TOPIC", "uk.gov.ons.dp.web.publish-count")
	dbSource := utils.GetEnvironmentVariable("DB_ACCESS", "user=dp dbname=dp sslmode=disable")
	restartGap, err := utils.GetEnvironmentVariableInt("RESEND_AFTER_QUIET_SECONDS", 0)
	if err != nil {
		log.Panicf("Failed to parse RESEND_AFTER_QUIET_SECONDS: %s", err)
	}
	restartGapNano := int64(restartGap * 1000 * 1000 * 1000)

	db, err := sql.Open("postgres", dbSource)
	if err != nil {
		log.Panicf("DB open error: %s", err.Error())
	}
	if err = db.Ping(); err != nil {
		log.Panicf("Error: Could not establish a connection with the database: %s", err.Error())
	}
	dbMeta := dbMetaObj{db: db, prepped: make(map[string]*sql.Stmt)}
	dbMeta.prep("load-incomplete-files", "SELECT schedule_file_id, file_path FROM schedule_file WHERE schedule_id=$1 AND complete_time IS NULL")
	dbMeta.prep("load-incomplete-deletes", "SELECT schedule_delete_id, delete_path FROM schedule_delete WHERE schedule_id=$1 AND complete_time IS NULL")
	if restartGap == 0 {
		dbMeta.prep("select-ready", "UPDATE schedule s SET start_time=$1 WHERE complete_time IS NULL AND start_time IS NULL AND schedule_time <= $1 RETURNING schedule_id, start_time, schedule_time, collection_id, collection_path, encryption_key")
	} else {
		dbMeta.prep("select-ready", "UPDATE schedule s SET start_time=$1 WHERE complete_time IS NULL AND schedule_time <= $1 AND (start_time IS NULL OR (start_time <= $2 AND NOT EXISTS(SELECT 1 FROM schedule_file sf WHERE s.schedule_id=sf.schedule_id AND sf.complete_time > $2))) RETURNING schedule_id, start_time, schedule_time, collection_id, collection_path, encryption_key")
	}

	log.Printf("Starting publish scheduler from %q topics: %q -> %q/%q", zebedeeRoot, scheduleTopic, produceFileTopic, produceTotalTopic)

	totalProducer := kafka.NewProducer(produceTotalTopic)
	scheduleConsumer := kafka.NewConsumerGroup(scheduleTopic, "publish-scheduler")
	fileProducer := kafka.NewProducer(produceFileTopic)
	deleteProducer := kafka.NewProducer(produceDeleteTopic)

	publishChannel := make(chan scheduleJob)
	exitChannel := make(chan bool)

	go func() {
		tock := time.Tick(tick)
		for _ = range tock {
			checkSchedule(publishChannel, dbMeta, restartGapNano)
		}
	}()

	go func() {
		for {
			select {
			case scheduleMessage := <-scheduleConsumer.Incoming:
				scheduleCollection(scheduleMessage.GetData(), zebedeeRoot, dbMeta)
				scheduleMessage.Commit()
			case publishMessage := <-publishChannel:
				go publishCollection(publishMessage, fileProducer.Output, deleteProducer.Output, totalProducer.Output)
			}
		}
	}()
	<-exitChannel
	log.Printf("Service publish scheduler stopped")
}
