producer:
	kafka-console-producer --broker-list localhost:9092 --topic uk.gov.ons.dp.web.schedule
receiver scheduler sender tracker:
	cd publish-$@ && go run publish-$@.go
migrator:
	cd static-content-migrator && go run content-migrator.go
