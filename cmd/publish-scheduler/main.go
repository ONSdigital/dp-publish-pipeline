package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/ONSdigital/dp-publish-pipeline/kafka"
	"github.com/ONSdigital/dp-publish-pipeline/utils"
	"github.com/ONSdigital/dp-publish-pipeline/vault"

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
	scheduleTime   int64
	encryptionKey  string
	files          []kafka.FileResource
	urisToDelete   []kafka.FileResource
}

func publishCollection(job scheduleJob, fileProducerChannel, deleteProducerChannel, totalProducerChannel chan []byte) {
	if job.collectionId == "" {
		log.Panicf("Bad message: %v", job)
	}

	var data []byte
	var err error

	// Send published files to the kafka topic
	for i := 0; i < len(job.files); i++ {
		if data, err = json.Marshal(kafka.PublishFileMessage{
			ScheduleId:     job.scheduleId,
			FileId:         job.files[i].Id,
			CollectionId:   job.collectionId,
			CollectionPath: job.collectionPath,
			EncryptionKey:  job.encryptionKey,
			FileLocation:   job.files[i].Location,
			Uri:            job.files[i].Uri,
		}); err != nil {
			log.Panic(err)
		}
		fileProducerChannel <- data
	}
	log.Printf("Job %d Collection %q sent %d files", job.scheduleId, job.collectionId, len(job.files))

	// Send published deletes to the kafka topic
	for i := 0; i < len(job.urisToDelete); i++ {
		if data, err = json.Marshal(kafka.PublishDeleteMessage{
			ScheduleId:   job.scheduleId,
			DeleteId:     job.urisToDelete[i].Id,
			Uri:          job.urisToDelete[i].Uri,
			CollectionId: job.collectionId,
		}); err != nil {
			log.Panic(err)
		}
		deleteProducerChannel <- data
	}
	log.Printf("Job %d Collection %q sent %d deletes", job.scheduleId, job.collectionId, len(job.urisToDelete))
}

func scheduleCollection(jsonMessage []byte, dbMeta dbMetaObj) {
	var message kafka.ScheduleMessage
	if err := json.Unmarshal(jsonMessage, &message); err != nil {
		log.Panicf("Failed to parse json: %v", jsonMessage)
	} else if len(message.CollectionId) == 0 || message.Action == "" {
		log.Panicf("Empty collectionId/action: %v", jsonMessage)
	}

	scheduleTime, err := strconv.ParseInt(message.ScheduleTime, 10, 64)
	if err != nil {
		log.Panicf("Collection %q Cannot numeric convert: %q", message.CollectionId, message.ScheduleTime)
	}
	scheduleTime *= 1000 * 1000 * 1000 // convert from epoch (seconds) to epoch-nanoseconds (UnixNano)

	if message.Action == "cancel" {
		cancelJob(dbMeta, message.CollectionId, scheduleTime)
	} else if message.Action == "schedule" {
		newJob := scheduleJob{
			collectionId:   message.CollectionId,
			collectionPath: message.CollectionPath,
			scheduleTime:   scheduleTime,
		}

		var files []kafka.FileResource
		for i := 0; i < len(message.Files); i++ {
			files = append(files, kafka.FileResource{Location: message.Files[i].Location, Uri: message.Files[i].Uri})
		}
		newJob.files = make([]kafka.FileResource, len(files))
		copy(newJob.files, files)

		var deletes []kafka.FileResource
		for i := 0; i < len(message.UrisToDelete); i++ {
			log.Println(message.UrisToDelete[i])
			deletes = append(deletes, kafka.FileResource{Uri: message.UrisToDelete[i]})
		}
		newJob.urisToDelete = make([]kafka.FileResource, len(deletes))
		copy(newJob.urisToDelete, deletes)

		newJob.scheduleId = storeJob(dbMeta, &newJob)
		log.Printf("Job %d Collection %q scheduled: %d files, %d deletes", newJob.scheduleId, newJob.collectionId, len(newJob.files), len(newJob.urisToDelete))
	} else {
		log.Panicf("Collection %q No/invalid Action: %v", message.CollectionId, message)
	}
}

func checkSchedule(publishChannel chan scheduleJob, dbMeta dbMetaObj, restartGapNano int64, vaultClient *vault.VaultClient) {
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
			collectionId, collectionPath        sql.NullString
			scheduleId, startTime, scheduleTime sql.NullInt64
		)

		if err = rows.Scan(&scheduleId, &startTime, &scheduleTime, &collectionId, &collectionPath); err != nil {
			log.Panic(err)
		}

		if maxLaunchPerTick > 0 && launchedThisTick >= maxLaunchPerTick {
			log.Printf("Job %d Collection %q skip busy - this-tick: %d/%d", scheduleId.Int64, collectionId.String, launchedThisTick, maxLaunchPerTick)
			continue
		}
		files := loadDataFromDataBase(dbMeta, scheduleId.Int64, false)
		deletes := loadDataFromDataBase(dbMeta, scheduleId.Int64, true)
		log.Printf("%d files, %d deletes", len(files), len(deletes))
		jobToGo := scheduleJob{
			scheduleId:     scheduleId.Int64,
			collectionId:   collectionId.String,
			collectionPath: collectionPath.String,
			scheduleTime:   scheduleTime.Int64,
			encryptionKey:  getEncryptionKeyFromVault(collectionId.String, vaultClient),
			files:          files,
			urisToDelete:   deletes,
		}
		publishChannel <- jobToGo
		launchedThisTick++
		log.Printf("Job %d Collection %q launch#%d with %d files at time:%d", scheduleId.Int64, collectionId.String, launchedThisTick, len(files), epochTime)
	}
	if verboseTick && launchedThisTick == 0 {
		log.Printf("No collections ready at %d", epochTime)
	}
}

func loadDataFromDataBase(dbMeta dbMetaObj, scheduleId int64, loadDeletes bool) []kafka.FileResource {
	sqlStmt := "load-incomplete-files"
	if loadDeletes {
		sqlStmt = "load-incomplete-deletes"
	}
	rows, err := dbMeta.prepped[sqlStmt].Query(scheduleId)
	if err != nil {
		log.Panic(err)
	}
	defer rows.Close()

	var files []kafka.FileResource
	for rows.Next() {
		var (
			fileId            sql.NullInt64
			fileLocation, uri sql.NullString
		)
		if err = rows.Scan(&fileId, &uri, &fileLocation); err != nil {
			log.Panic(err)
		}

		files = append(files, kafka.FileResource{Id: fileId.Int64, Location: fileLocation.String, Uri: uri.String})
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

	if jobStmt, err = txn.Prepare("INSERT INTO schedule (collection_id, collection_path, schedule_time, start_time, complete_time) VALUES ($1, $2, $3, NULL, NULL) RETURNING schedule_id"); err != nil {
		rollbackAndError(txn, err)
	}
	if fileStmt, err = txn.Prepare("INSERT INTO schedule_file (schedule_id, uri, file_location) VALUES ($1, $2, $3) RETURNING schedule_file_id"); err != nil {
		rollbackAndError(txn, err)
	}
	if deleteStmt, err = txn.Prepare("INSERT INTO schedule_delete (schedule_id, uri) VALUES ($1, $2) RETURNING schedule_delete_id"); err != nil {
		rollbackAndError(txn, err)
	}

	// insert job into schedule
	res := jobStmt.QueryRow((*jobObj).collectionId, (*jobObj).collectionPath, (*jobObj).scheduleTime)
	if err = res.Scan(&scheduleId); err != nil {
		rollbackAndError(txn, err)
	}
	// insert files into schedule_file
	for i := 0; i < len((*jobObj).files); i++ {
		if fileId, err = storeFile(fileStmt, scheduleId.Int64, (*jobObj).files[i].Uri, (*jobObj).files[i].Location); err != nil {
			rollbackAndError(txn, err)
		}
		(*jobObj).files[i].Id = fileId
	}

	// insert deleted files into schedule_delete
	for i := 0; i < len((*jobObj).urisToDelete); i++ {
		if _, err = storeFile(deleteStmt, scheduleId.Int64, (*jobObj).urisToDelete[i].Uri, ""); err != nil {
			rollbackAndError(txn, err)
		}
		(*jobObj).urisToDelete[i].Id = fileId
	}

	if err = txn.Commit(); err != nil {
		rollbackAndError(txn, err)
	}

	return scheduleId.Int64
}

func storeFile(stmt *sql.Stmt, scheduleId int64, uri, location string) (int64, error) {
	res := stmt.QueryRow(scheduleId, uri, location)
	var fileId sql.NullInt64
	if err := res.Scan(&fileId); err != nil {
		return 0, err
	}
	return fileId.Int64, nil
}

func rollbackAndError(txn *sql.Tx, err error) {
	if err2 := txn.Rollback(); err2 != nil {
		log.Panicf("Error during rollback %s (while handling: %s)", err2, err)
	}
	log.Panic(err)
}

func cancelJob(dbMeta dbMetaObj, collectionId string, scheduleTime int64) {
	var (
		txn        *sql.Tx
		jobStmt    *sql.Stmt
		err        error
		scheduleId sql.NullInt64
	)

	if txn, err = dbMeta.db.Begin(); err != nil {
		log.Panic(err)
	}

	if jobStmt, err = txn.Prepare("DELETE FROM schedule WHERE collection_id=$1 AND schedule_time=$2 AND start_time IS NULL RETURNING schedule_id"); err != nil {
		rollbackAndError(txn, err)
	}

	// delete job(s) from schedule
	rows, err := jobStmt.Query(collectionId, scheduleTime)
	if err != nil {
		rollbackAndError(txn, err)
	}

	var scheduleIds []interface{}
	placeholder := ""
	for rows.Next() {
		if err = rows.Scan(&scheduleId); err != nil {
			rollbackAndError(txn, err)
		}
		scheduleIds = append(scheduleIds, scheduleId.Int64)
		if len(placeholder) > 0 {
			placeholder += ","
		}
		placeholder += "$" + strconv.Itoa(len(scheduleIds))
	}

	if len(scheduleIds) > 0 {
		// delete files from schedule_file
		if _, err := txn.Exec("DELETE FROM schedule_file WHERE schedule_id IN ("+placeholder+")", scheduleIds...); err != nil {
			rollbackAndError(txn, fmt.Errorf("Jobs %v Collection %q Cannot delete files: %s", scheduleIds, collectionId, err))
		}
		// delete files from schedule_delete
		if _, err := txn.Exec("DELETE FROM schedule_delete WHERE schedule_id IN ("+placeholder+")", scheduleIds...); err != nil {
			rollbackAndError(txn, fmt.Errorf("Jobs %v Collection %q Cannot delete file-deletes: %s", scheduleIds, collectionId, err))
		}

		log.Printf("Jobs %v Collection %q at %d CANCELLED", scheduleIds, collectionId, scheduleTime)

	} else {
		log.Printf("Job ?? Collection %q at %d not found to cancel", collectionId, scheduleTime)
	}

	if err = txn.Commit(); err != nil {
		rollbackAndError(txn, err)
	}
}

func getEncryptionKeyFromVault(collectionId string, vaultClient *vault.VaultClient) string {
	data, err := vaultClient.Read("secret/zebedee-cms/" + collectionId)
	if err != nil {
		log.Panicf("Failed to find encryption key : %s", err.Error())
	}
	key, ok := data["encryption_key"].(string)
	if ok {
		return key
	}
	return ""
}

func (dbMeta dbMetaObj) prep(tag, sql string) {
	var err error
	dbMeta.prepped[tag], err = dbMeta.db.Prepare(sql)
	if err != nil {
		log.Panicf("Error: Could not prepare %q statement on database: %s", tag, err.Error())
	}
}

func main() {

	scheduleTopic := utils.GetEnvironmentVariable("SCHEDULE_TOPIC", "uk.gov.ons.dp.web.schedule")
	produceFileTopic := utils.GetEnvironmentVariable("PUBLISH_FILE_TOPIC", "uk.gov.ons.dp.web.publish-file")
	produceDeleteTopic := utils.GetEnvironmentVariable("PUBLISH_DELETE_TOPIC", "uk.gov.ons.dp.web.publish-delete")
	produceTotalTopic := utils.GetEnvironmentVariable("PUBLISH_COUNT_TOPIC", "uk.gov.ons.dp.web.publish-count")
	dbSource := utils.GetEnvironmentVariable("DB_ACCESS", "user=dp dbname=dp sslmode=disable")
	restartGap, err := utils.GetEnvironmentVariableInt("RESEND_AFTER_QUIET_SECONDS", 0)
	vaultToken := utils.GetEnvironmentVariable("VAULT_TOKEN", "")
	vaultAddr := utils.GetEnvironmentVariable("VAULT_ADDR", "http://127.0.0.1:8200")
	vaultRenewTime, err := utils.GetEnvironmentVariableInt("VAULT_RENEW_TIME", 5)

	vaultClient, err := vault.CreateVaultClient(vaultToken, vaultAddr)
	if err != nil {
		log.Panicf("Failed to connect to vault : %s", err.Error())
	}

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
	dbMeta.prep("load-incomplete-files", "SELECT schedule_file_id, uri, file_location FROM schedule_file WHERE schedule_id=$1 AND complete_time IS NULL")
	dbMeta.prep("load-incomplete-deletes", "SELECT schedule_delete_id, uri FROM schedule_delete WHERE schedule_id=$1 AND complete_time IS NULL")
	if restartGap == 0 {
		dbMeta.prep("select-ready", "UPDATE schedule s SET start_time=$1 WHERE complete_time IS NULL AND start_time IS NULL AND schedule_time <= $1 RETURNING schedule_id, start_time, schedule_time, collection_id, collection_path")
	} else {
		dbMeta.prep("select-ready", "UPDATE schedule s SET start_time=$1 WHERE complete_time IS NULL AND schedule_time <= $1 AND (start_time IS NULL OR (start_time <= $2 AND NOT EXISTS(SELECT 1 FROM schedule_file sf WHERE s.schedule_id=sf.schedule_id AND sf.complete_time > $2))) RETURNING schedule_id, start_time, schedule_time, collection_id, collection_path")
	}

	log.Printf("Starting publish scheduler topics: %q -> %q/%q/%q", scheduleTopic, produceFileTopic, produceDeleteTopic, produceTotalTopic)

	totalProducer := kafka.NewProducer(produceTotalTopic)
	scheduleConsumer, err := kafka.NewConsumerGroup(scheduleTopic, "publish-scheduler")
	if err != nil {
		log.Panicf("Could not obtain consumer: %s", err)
	}
	fileProducer := kafka.NewProducer(produceFileTopic)
	deleteProducer := kafka.NewProducer(produceDeleteTopic)

	publishChannel := make(chan scheduleJob)
	exitChannel := make(chan bool)

	go func() {
		tock := time.Tick(tick)
		for _ = range tock {
			checkSchedule(publishChannel, dbMeta, restartGapNano, vaultClient)
		}
	}()

	go func() {
		tock := time.Tick(time.Duration(vaultRenewTime) * time.Minute)
		for _ = range tock {
			err := vaultClient.Renew()
			if err != nil {
				log.Panicf("Failed to renew vault token : %s", err.Error())
			}
			log.Printf("Renewed vault token")
		}
	}()

	go func() {
		for {
			select {
			case scheduleMessage := <-scheduleConsumer.Incoming:
				scheduleCollection(scheduleMessage.GetData(), dbMeta)
				scheduleMessage.Commit()
			case publishMessage := <-publishChannel:
				go publishCollection(publishMessage, fileProducer.Output, deleteProducer.Output, totalProducer.Output)
			case errorMessage := <-scheduleConsumer.Errors:
				log.Printf("Aborting: %s", errorMessage)
				exitChannel <- true
				return
			}
		}
	}()
	<-exitChannel
	log.Printf("Service publish scheduler stopped")
}
