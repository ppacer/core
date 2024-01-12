package scheduler

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/dskrzypiec/scheduler/dag"
	"github.com/dskrzypiec/scheduler/db"
	"github.com/dskrzypiec/scheduler/ds"
	"github.com/dskrzypiec/scheduler/timeutils"
)

func TestNextScheduleForDagRunsSimple(t *testing.T) {
	c, err := db.NewInMemoryClient(sqlSchemaPath)
	if err != nil {
		t.Fatal(err)
	}
	const dagRuns = 10
	const dagId = "mock_dag"
	ctx := context.Background()

	startTs := time.Date(2023, time.October, 5, 12, 0, 0, 0, time.UTC)
	sched := dag.FixedSchedule{Interval: 1 * time.Hour, Start: startTs}
	attr := dag.Attr{}
	d := emptyDag(dagId, &sched, attr)

	for i := 0; i < dagRuns; i++ {
		_, err := c.InsertDagRun(ctx, dagId,
			timeutils.ToString(startTs.Add(time.Duration(i)*time.Hour)))
		if err != nil {
			t.Errorf("Error while inserting dagrun: %s", err.Error())
		}
	}

	currentTime := startTs.Add(time.Duration(dagRuns)*time.Hour + 45*time.Minute)
	nextSchedulesMap := make(map[dag.Id]*time.Time)
	updateNextSchedules(ctx, []dag.Dag{d}, currentTime, c, nextSchedulesMap)

	if len(nextSchedulesMap) != 1 {
		t.Errorf("Expected to got next schedule for single DAG, got for %d",
			len(nextSchedulesMap))
	}

	nextSchedule, exists := nextSchedulesMap[d.Id]
	if !exists {
		t.Errorf("Expected DAG %s to exist in nextSchedulesMap, but it does not",
			dagId)
	}
	expectedNextSchedule := startTs.Add(time.Duration(dagRuns) * time.Hour)
	if nextSchedule.Compare(expectedNextSchedule) != 0 {
		t.Errorf("Expected next schedule for DAG %s for the current time %v to be %v, but got %v",
			dagId, currentTime, expectedNextSchedule, nextSchedule)
	}
}

func TestNextScheduleForDagRunsSimpleWithCatchUp(t *testing.T) {
	c, err := db.NewInMemoryClient(sqlSchemaPath)
	if err != nil {
		t.Fatal(err)
	}
	const dagRuns = 1
	const dagId = "mock_dag"
	ctx := context.Background()

	startTs := time.Date(2023, time.October, 5, 12, 0, 0, 0, time.UTC)
	sched := dag.FixedSchedule{Interval: 1 * time.Hour, Start: startTs}
	attr := dag.Attr{CatchUp: true}
	d := emptyDag(dagId, &sched, attr)

	for i := 0; i < dagRuns; i++ {
		_, err := c.InsertDagRun(ctx, dagId,
			timeutils.ToString(startTs.Add(time.Duration(i)*time.Hour)))
		if err != nil {
			t.Errorf("Error while inserting dagrun: %s", err.Error())
		}
	}

	currentTime := time.Date(2023, time.October, 10, 10, 0, 0, 0, time.UTC)
	nextSchedulesMap := make(map[dag.Id]*time.Time)
	updateNextSchedules(ctx, []dag.Dag{d}, currentTime, c, nextSchedulesMap)

	if len(nextSchedulesMap) != 1 {
		t.Errorf("Expected to got next schedule for single DAG, got for %d",
			len(nextSchedulesMap))
	}

	nextSchedule, exists := nextSchedulesMap[d.Id]
	if !exists {
		t.Errorf("Expected DAG %s to exist in nextSchedulesMap, but it does not",
			dagId)
	}
	expectedNextSchedule := startTs.Add(1 * time.Hour)
	if nextSchedule.Compare(expectedNextSchedule) != 0 {
		t.Errorf("Expected next schedule for DAG %s for the current time %v to be %v, but got %v",
			dagId, currentTime, expectedNextSchedule, nextSchedule)
	}
}

func TestNextScheduleForDagRunsManyDagsSimple(t *testing.T) {
	c, err := db.NewInMemoryClient(sqlSchemaPath)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	attr := dag.Attr{}

	start := time.Date(2023, time.October, 5, 12, 0, 0, 0, time.UTC)
	sched1 := dag.FixedSchedule{Interval: 1 * time.Hour, Start: start}
	sched2 := dag.FixedSchedule{Interval: 5 * time.Hour, Start: start}
	sched3 := dag.FixedSchedule{Interval: 10 * time.Minute, Start: start}

	d1 := emptyDag("dag1", &sched1, attr)
	d2 := emptyDag("dag2", &sched2, attr)
	d3 := emptyDag("dag3", &sched3, attr)

	for _, dagId := range []string{"dag1", "dag2", "dag3"} {
		_, err := c.InsertDagRun(ctx, dagId, timeutils.ToString(start))
		if err != nil {
			t.Errorf("Error while inserting dagrun: %s", err.Error())
		}
	}

	currentTime := start.Add(5 * time.Minute)
	nextSchedulesMap := make(map[dag.Id]*time.Time)
	updateNextSchedules(ctx, []dag.Dag{d1, d2, d3}, currentTime, c, nextSchedulesMap)

	if len(nextSchedulesMap) != 3 {
		t.Errorf("Expected to got next schedule for single DAG, got for %d",
			len(nextSchedulesMap))
	}

	expectedNextScheds := []time.Time{
		start.Add(1 * time.Hour),
		start.Add(5 * time.Hour),
		start.Add(10 * time.Minute),
	}

	for idx, d := range []dag.Dag{d1, d2, d3} {
		nextSched, exists := nextSchedulesMap[d.Id]
		if !exists {
			t.Errorf("Expected DAG %s to exist in nextSchedulesMap, but it does not",
				string(d.Id))
		}
		if nextSched.Compare(expectedNextScheds[idx]) != 0 {
			t.Errorf("Expected next schedule for DAG %s for the current time %v to be %v, but got %v",
				string(d.Id), currentTime, expectedNextScheds[idx], nextSched)
		}
	}
}

func TestNextScheduleForDagRunsBeforeStart(t *testing.T) {
	c, err := db.NewInMemoryClient(sqlSchemaPath)
	if err != nil {
		t.Fatal(err)
	}
	dagNumber := 100
	ctx := context.Background()
	dags := make([]dag.Dag, 0, dagNumber)
	attr := dag.Attr{}

	for i := 0; i < dagNumber; i++ {
		start := timeutils.RandomUtcTime(2010)
		h := rand.Intn(1000)
		sched := dag.FixedSchedule{
			Interval: time.Duration(h) * time.Hour,
			Start:    start,
		}
		d := emptyDag(fmt.Sprintf("d_%d", i), &sched, attr)
		dags = append(dags, d)
	}

	currentTime := time.Date(2008, time.October, 5, 12, 0, 0, 0, time.UTC)
	nextSchedulesMap := make(map[dag.Id]*time.Time)
	updateNextSchedules(ctx, dags, currentTime, c, nextSchedulesMap)

	if len(nextSchedulesMap) != dagNumber {
		t.Errorf("Expected to got next schedule for %d DAGs, got for %d",
			dagNumber, len(nextSchedulesMap))
	}

	for _, d := range dags {
		nextSched, exists := nextSchedulesMap[d.Id]
		if !exists {
			t.Errorf("Expected DAG %s to exist in nextSchedulesMap, but it does not",
				string(d.Id))
		}
		expectedNextSched := (*d.Schedule).StartTime()
		if nextSched.Compare(expectedNextSched) != 0 {
			t.Errorf("Expected next schedule for DAG %s for the current time %v to be %v, but got %v",
				string(d.Id), currentTime, expectedNextSched, nextSched)
		}
	}
}

func TestNextScheduleForDagRunsNoSchedule(t *testing.T) {
	c, err := db.NewInMemoryClient(sqlSchemaPath)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	d1 := dag.New(dag.Id("d1")).Done()
	d2 := dag.New(dag.Id("s2")).Done()

	currentTime := time.Date(2008, time.October, 5, 12, 0, 0, 0, time.UTC)
	nextSchedulesMap := make(map[dag.Id]*time.Time)
	updateNextSchedules(ctx, []dag.Dag{d1, d2}, currentTime, c, nextSchedulesMap)

	if len(nextSchedulesMap) != 2 {
		t.Errorf("Expected to got next schedule for %d DAGs, got for %d", 2,
			len(nextSchedulesMap))
	}

	for dagId, nextSched := range nextSchedulesMap {
		if nextSched != nil {
			t.Errorf("Expected nil next schedule for %s DAG, got %v",
				string(dagId), nextSched)
		}
	}
}

func TestShouldBeScheduledSimple(t *testing.T) {
	attr := dag.Attr{}
	start := time.Date(2023, time.October, 5, 12, 0, 0, 0, time.UTC)
	sched1 := dag.FixedSchedule{Interval: 1 * time.Hour, Start: start}

	d1 := emptyDag("dag1", &sched1, attr)
	d2 := dag.New(dag.Id("dag4")).Done()

	d1ns := time.Date(2023, time.October, 5, 14, 0, 0, 0, time.UTC)
	nextSchedules := map[dag.Id]*time.Time{
		d1.Id: &d1ns,
		d2.Id: nil,
	}

	// test
	currTime1 := time.Date(2023, time.October, 5, 13, 59, 0, 0, time.UTC)
	currTime2 := time.Date(2023, time.October, 5, 14, 0, 1, 0, time.UTC)

	shouldBe1, execTime1 := shouldBeSheduled(d1, nextSchedules, currTime1)
	if shouldBe1 {
		t.Errorf("Dag %s should not be scheduled at %v, but shouldBeScheduled returned true, %v",
			string(d1.Id), currTime1, execTime1)
	}
	shouldBe2, execTime2 := shouldBeSheduled(d1, nextSchedules, currTime2)
	if !shouldBe2 {
		t.Errorf("Dag %s should be scheduled at %v, but shouldBeSheduled returned false",
			string(d1.Id), currTime2)
	}
	if execTime2.Compare(d1ns) != 0 {
		t.Errorf("Expected DAG %s to be scheduled at %v, but got %v",
			string(d1.Id), d1ns, execTime2)
	}
	ds1NextNextSched, exists := nextSchedules[d1.Id]
	if !exists {
		t.Errorf("Expected DAG %s next schedule to exist in the map, but it does not",
			string(d1.Id))
	}
	if ds1NextNextSched == nil {
		t.Fatalf("Expected non-nil next schedule after already checking shouldBeSheduled for DAG %s",
			string(d1.Id))
	}
	if d1ns.Compare(*ds1NextNextSched) != 0 {
		t.Errorf("Expected next schedule after once checking shouldBeScheduled for DAG %s, to be %v, got %v",
			string(d1.Id), d1ns, *ds1NextNextSched)
	}

	for _, ct := range []time.Time{currTime1, currTime2} {
		shouldBe, execTime := shouldBeSheduled(d2, nextSchedules, ct)
		if shouldBe {
			t.Errorf("Expected no next schedule time for DAG without schedule, got: %v",
				execTime)
		}
	}
}

func TestShouldBeScheduledExactlyOnScheduleTime(t *testing.T) {
	attr := dag.Attr{}
	start := time.Date(2023, time.October, 5, 12, 0, 0, 0, time.UTC)
	sched1 := dag.FixedSchedule{Interval: 1 * time.Hour, Start: start}

	d1 := emptyDag("dag1", &sched1, attr)
	d1ns := start
	nextSchedules := map[dag.Id]*time.Time{d1.Id: &d1ns}

	// test
	const iterations = 25
	currTime := start

	for i := 0; i < iterations; i++ {
		shouldBe, execTime := shouldBeSheduled(d1, nextSchedules, currTime)
		if !shouldBe {
			t.Errorf("Dag %s should be scheduled at %v, but shouldBeSheduled returned false",
				string(d1.Id), currTime)
		}
		if currTime.Compare(execTime) != 0 {
			t.Errorf("Expected execTime for DAG %s to be %v, got %v",
				string(d1.Id), currTime, execTime)
		}
		nextCurrTime, exists := nextSchedules[d1.Id]
		if !exists {
			t.Errorf("Expected DAG %s next schedule to exist in the map, but it does not",
				string(d1.Id))
		}
		if nextCurrTime == nil {
			t.Fatalf("Expected non-nil next schedule after already checking shouldBeSheduled for DAG %s",
				string(d1.Id))
		}
		currTime = *nextCurrTime
	}
}

func TestShouldBeScheduledUnexpectedDelay(t *testing.T) {
	const dagId = "mock_dag"
	startTs := time.Date(2023, time.October, 5, 12, 0, 0, 0, time.UTC)
	sched := dag.FixedSchedule{Interval: 1 * time.Hour, Start: startTs}
	attr := dag.Attr{}
	d := emptyDag(dagId, &sched, attr)

	nextSched := time.Date(2023, time.November, 13, 10, 0, 0, 0, time.UTC)
	nextSchedules := map[dag.Id]*time.Time{d.Id: &nextSched}

	// We are simulating that for over 3 hours dag run queue was full and now
	// current time is 13:05 instead of 10:00.
	currentTime := time.Date(2023, time.November, 13, 13, 5, 0, 0, time.UTC)
	shouldBe, nextSched := shouldBeSheduled(d, nextSchedules, currentTime)
	if !shouldBe {
		t.Errorf("Expected DAG to be scheduled at %+v, but it's not",
			currentTime)
	}

	expectedNextSched := time.Date(2023, time.November, 13, 10, 0, 0, 0, time.UTC)
	if !nextSched.Equal(expectedNextSched) {
		t.Errorf("Expected DAG to be scheduled with ExecTs %+v, but got: %+v",
			expectedNextSched, nextSched)
	}
}

func TestShouldBeScheduledEmtpyNextMap(t *testing.T) {
	const N = 25
	attr := dag.Attr{}
	start := time.Date(2023, time.October, 5, 12, 0, 0, 0, time.UTC)
	sched1 := dag.FixedSchedule{Interval: 1 * time.Hour, Start: start}
	d1 := emptyDag("dag1", &sched1, attr)

	for i := 0; i < N; i++ {
		ct := timeutils.RandomUtcTime(2020)
		shouldBe, _ := shouldBeSheduled(d1, map[dag.Id]*time.Time{}, ct)
		if shouldBe {
			t.Error("Expected DAG to not be sheduled when map of next schedules is empty, but shouldBeScheduled return true")
		}
	}
}

func TestTryScheduleDagSimple(t *testing.T) {
	c, err := db.NewInMemoryClient(sqlSchemaPath)
	if err != nil {
		t.Fatal(err)
	}

	attr := dag.Attr{}
	start := time.Date(2023, time.October, 5, 12, 0, 0, 0, time.UTC)
	sched1 := dag.FixedSchedule{Interval: 1 * time.Hour, Start: start}

	d1 := emptyDag("dag1", &sched1, attr)

	d1ns := time.Date(2023, time.October, 5, 14, 0, 0, 0, time.UTC)
	nextSchedules := map[dag.Id]*time.Time{d1.Id: &d1ns}
	queue := ds.NewSimpleQueue[DagRun](100)

	timePoints := []time.Time{
		time.Date(2023, time.October, 5, 13, 0, 0, 0, time.UTC),   // no schedule
		time.Date(2023, time.October, 5, 14, 0, 1, 0, time.UTC),   // new dag run
		time.Date(2023, time.October, 5, 14, 59, 59, 0, time.UTC), // no action
		time.Date(2023, time.October, 5, 15, 1, 1, 0, time.UTC),   // new dag run
		time.Date(2023, time.October, 5, 15, 2, 0, 0, time.UTC),   // no action
		time.Date(2023, time.October, 5, 16, 30, 0, 0, time.UTC),  // new dag run
	}

	// test
	for _, currTime := range timePoints {
		ctx := context.Background()
		err := tryScheduleDag(ctx, d1, currTime, &queue, nextSchedules, c)
		if err != nil {
			t.Errorf("Error while trying to schedule new dag run: %s",
				err.Error())
		}
	}
	const expectedDagRuns = 3
	dbDagruns := c.Count("dagruns")
	if dbDagruns != expectedDagRuns {
		t.Errorf("Expected %d dag runs in dagruns table, got: %d",
			expectedDagRuns, dbDagruns)
	}
	if queue.Size() != expectedDagRuns {
		t.Errorf("Expected %d dag runs on the queue, got: %d",
			expectedDagRuns, queue.Size())
	}
	expectedExecTimes := []time.Time{
		time.Date(2023, time.October, 5, 14, 0, 0, 0, time.UTC),
		time.Date(2023, time.October, 5, 15, 0, 0, 0, time.UTC),
		time.Date(2023, time.October, 5, 16, 0, 0, 0, time.UTC),
	}
	for idx, expExecTime := range expectedExecTimes {
		dr, pErr := queue.Pop()
		if pErr != nil {
			t.Errorf("Error while trying to pop element %d from the queue: %s",
				idx+1, pErr.Error())
		}
		if expExecTime.Compare(dr.AtTime) != 0 {
			t.Errorf("Expected for dag run %d exec time %v, got: %v", idx+1,
				expExecTime, dr.AtTime)
		}
	}

	ctx := context.Background()
	dbDagRuns, dbErr := c.ReadDagRuns(ctx, string(d1.Id), expectedDagRuns)
	if dbErr != nil {
		t.Errorf("Error while reading dagruns from database: %s", dbErr.Error())
	}
	if len(dbDagRuns) != expectedDagRuns {
		t.Fatalf("Expected %d dag runs in dagruns table, got: %d",
			expectedDagRuns, len(dbDagRuns))
	}
	for i := 0; i < expectedDagRuns; i++ {
		expTimeStr := timeutils.ToString(expectedExecTimes[expectedDagRuns-i-1])
		if dbDagRuns[i].ExecTs != expTimeStr {
			t.Errorf("Expected for dag run %d exec time %s, got %s in database",
				expectedDagRuns-i-1, expTimeStr, dbDagRuns[i].ExecTs)
		}
	}
}

func TestTryScheduleDagUnexpectedDelay(t *testing.T) {
	c, err := db.NewSqliteTmpClient()
	if err != nil {
		t.Fatal(err)
	}
	defer db.CleanUpSqliteTmp(c, t)

	ctx := context.Background()
	attr := dag.Attr{}
	start := time.Date(2023, time.October, 5, 12, 0, 0, 0, time.UTC)
	sched := dag.FixedSchedule{Interval: 1 * time.Hour, Start: start}
	d := emptyDag("sample_dag", &sched, attr)

	t1 := time.Date(2024, time.January, 7, 8, 0, 0, 0, time.UTC)
	nextSchedules := map[dag.Id]*time.Time{d.Id: &t1}
	queue := ds.NewSimpleQueue[DagRun](100)

	// Regular scheduling of a DAG run for d
	t2 := time.Date(2024, time.January, 7, 8, 0, 10, 0, time.UTC)
	s1Err := tryScheduleDag(ctx, d, t2, &queue, nextSchedules, c)
	if s1Err != nil {
		t.Errorf("Error while scheduling new DAG run at %+v", t2)
	}
	dr, popErr := queue.Pop()
	if popErr != nil {
		t.Errorf("Cannot pop item from the queue: %s", popErr.Error())
	}
	if !dr.AtTime.Equal(t1) {
		t.Errorf("Expected that scheduled DAG run is for %+v, but it's %+v",
			t1, dr.AtTime)
	}
	t2NextExp := time.Date(2024, time.January, 7, 9, 0, 0, 0, time.UTC)
	checkNextSchedule(nextSchedules, d, t2NextExp, t)

	// Let's simulate that DAG runs queue was full for 2 hours and try schedule
	// just after the pause at 10:01:20.
	tAfterDelay := time.Date(2024, time.January, 7, 10, 1, 20, 0, time.UTC)
	s2Err := tryScheduleDag(ctx, d, tAfterDelay, &queue, nextSchedules, c)
	if s2Err != nil {
		t.Errorf("Error while scheduling new DAG run at %+v (after 2h delay)",
			tAfterDelay)
	}
	dr2, pop2Err := queue.Pop()
	if pop2Err != nil {
		t.Errorf("Cannot pop item from the queue  after the delay: %s",
			pop2Err.Error())
	}
	if !dr2.AtTime.Equal(t2NextExp) {
		t.Errorf("Expected that DAG run after the delay will be scheduled for %+v, but it's %+v",
			t2NextExp, dr2.AtTime)
	}
	t3NextExp := time.Date(2024, time.January, 7, 10, 0, 0, 0, time.UTC)
	checkNextSchedule(nextSchedules, d, t3NextExp, t)
}

func TestTryScheduleAfterSchedulerRestart(t *testing.T) {
	c, err := db.NewSqliteTmpClient()
	if err != nil {
		t.Fatal(err)
	}
	defer db.CleanUpSqliteTmp(c, t)

	ctx := context.Background()
	attr := dag.Attr{}
	start := time.Date(2023, time.October, 5, 12, 0, 0, 0, time.UTC)
	sched := dag.FixedSchedule{Interval: 1 * time.Hour, Start: start}
	d := emptyDag("sample_dag", &sched, attr)

	// we simulate empty nextSchedules after the scheduler restart (actually we
	// sync intentionally on scheduler.Start, but we also should have saftey
	// mechanism on trySchedule level to ensure that we always have
	// nextSchedules up to date).
	nextSchedules := map[dag.Id]*time.Time{}
	queue := ds.NewSimpleQueue[DagRun](10)

	// Insert one DAG run into the database as a state before the restart
	t0 := time.Date(2023, time.November, 11, 8, 0, 0, 0, time.UTC)
	runId, iErr := c.InsertDagRun(ctx, string(d.Id), timeutils.ToString(t0))
	if iErr != nil {
		t.Errorf("Cannot insert DAG run into the DB: %s", iErr.Error())
	}
	uErr := c.UpdateDagRunStatus(ctx, runId, dag.TaskSuccess.String())
	if uErr != nil {
		t.Errorf("Cannot update DAG run status: %s", uErr.Error())
	}

	// Starting scheduling DAG run after restart at 12:10 and latest run was at
	// 8:00.
	cfg := DefaultDagRunWatcherConfig
	timeAfterRestart := time.Date(2023, time.November, 11, 12, 10, 0, 0, time.UTC)
	trySchedule([]dag.Dag{d}, &queue, nextSchedules, timeAfterRestart, c, cfg)

	if len(nextSchedules) < 1 {
		t.Error("Expected at least one entry in nextSchedules, got 0")
	}

	// Expected DAG run for 9:00 after the restart, even though it's after
	// 12:00
	dr, popErr := queue.Pop()
	if popErr != nil {
		t.Errorf("Cannot pop DAG run from the queue: %s", popErr.Error())
	}
	currentSchedExp := time.Date(2023, time.November, 11, 9, 0, 0, 0, time.UTC)
	if !dr.AtTime.Equal(currentSchedExp) {
		t.Errorf("Expected to schedule DR at %+v after the restart, but got %+v",
			currentSchedExp, dr.AtTime)
	}

	// Next schedule should be 10:00 and not 13:00
	nextSchedExp := time.Date(2023, time.November, 11, 10, 0, 0, 0, time.UTC)
	nextSched, exist := nextSchedules[d.Id]
	if !exist {
		t.Errorf("Expected to have nextSchedule for DAG %s, but there is no entry",
			string(d.Id))
	}
	if !nextSchedExp.Equal(*nextSched) {
		t.Errorf("Expected nextSchedule after the restart to be %+v, but got: %+v",
			nextSchedExp, *nextSched)
	}
}

func checkNextSchedule(
	ns map[dag.Id]*time.Time, d dag.Dag, nextSchedExp time.Time, t *testing.T,
) {
	t.Helper()
	tNext, exists := ns[d.Id]
	if !exists {
		t.Errorf("Expected %s DAG to be in nextSchedules map, but it's not",
			string(d.Id))
	}
	if tNext == nil {
		t.Fatalf("Next schedule for %s is unexpectedly nil", string(d.Id))
	}
	if !nextSchedExp.Equal(*tNext) {
		t.Errorf("Expected nextSchedule to be %+v, but got %+v", nextSchedExp,
			*tNext)
	}
}

func emptyDag(dagId string, sched dag.Schedule, attr dag.Attr) dag.Dag {
	return dag.New(dag.Id(dagId)).AddSchedule(sched).AddAttributes(attr).Done()
}
