package main

import (
	"strconv"
	"strings"
)

type DataFilter struct {
	FromMonth   string
	ToMonth     string
	FromQuarter string
	ToQuarter   string
	FromYear    string
	ToYear      string
	Frequency   string
}

func filterOnYears(fileWriter FileWriter, timeSeries TimeSeries, filter DataFilter) {
	min, _ := strconv.Atoi(filter.FromYear)
	max, _ := strconv.Atoi(filter.ToYear)
	for _, data := range timeSeries.Years {
		currentYear, _ := strconv.Atoi(data.Year)
		if currentYear >= min && currentYear <= max {
			fileWriter([]string{data.Date, data.Value})
		}
	}
}

func filterOnQuarter(fileWriter FileWriter, timeSeries TimeSeries, filter DataFilter) {
	min, _ := strconv.Atoi(filter.FromYear)
	max, _ := strconv.Atoi(filter.ToYear)
	for _, data := range timeSeries.Quarters {
		currentYear, _ := strconv.Atoi(data.Year)
		if currentYear > min && currentYear < max {
			fileWriter([]string{data.Date, data.Value})
		} else if currentYear == min {
			minQuarter := quarterToNumber(filter.FromQuarter)
			currentQuarter := quarterToNumber(data.Quarter)
			if currentQuarter >= minQuarter {
				fileWriter([]string{data.Date, data.Value})
			}
		} else if currentYear == max {
			maxQuarter := quarterToNumber(filter.ToQuarter)
			currentQuarter := quarterToNumber(data.Quarter)
			if currentQuarter <= maxQuarter {
				fileWriter([]string{data.Date, data.Value})
			}
		}

	}
}

func quarterToNumber(quarter string) int {
	value, _ := strconv.Atoi(string(quarter[1]))
	return value
}

func filterOnMonth(fileWriter FileWriter, timeSeries TimeSeries, filter DataFilter) {
	min, _ := strconv.Atoi(filter.FromYear)
	max, _ := strconv.Atoi(filter.ToYear)
	for _, data := range timeSeries.Months {
		currentYear, _ := strconv.Atoi(data.Year)
		if currentYear > min && currentYear < max {
			fileWriter([]string{data.Date, data.Value})
		} else if currentYear == min {
			minQuarter, _ := strconv.Atoi(filter.FromMonth)
			currentQuarter := monthToNumber(data.Month)
			if currentQuarter >= minQuarter {
				fileWriter([]string{data.Date, data.Value})
			}
		} else if currentYear == max {
			maxQuarter, _ := strconv.Atoi(filter.ToMonth)
			currentQuarter := monthToNumber(data.Month)
			if currentQuarter <= maxQuarter {
				fileWriter([]string{data.Date, data.Value})
			}
		}

	}
}

func monthToNumber(month string) int {
	lowerCase := strings.ToLower(month)
	switch lowerCase {
	case "january":
		return 1
	case "february":
		return 2
	case "march":
		return 3
	case "april":
		return 4
	case "may":
		return 5
	case "june":
		return 6
	case "july":
		return 7
	case "august":
		return 8
	case "september":
		return 9
	case "october":
		return 10
	case "november":
		return 11
	case "december":
		return 12
	}

	return 0
}
