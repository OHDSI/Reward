test_that("Calibrate rows function works", {
  positives <- data.frame(rr = c(1.0, 2.0, 1.5), seLogRr = c(1.0, 1.2, 2.5))
  negatives <- data.frame(rr = c(1.0), seLogRr = c(1.0))

  result <- computeCalibratedRows(positives = positives,
                                  negatives = negatives,
                                  keepCols = NULL)

  checkmate::expect_data_frame(result,
                               nrows = nrow(positives),
                               col.names = "named")

  checkmate::expect_names(names(result), must.include = c("calibrated", "pValue", "ub95", "lb95", "rr", "seLogRr"))
  # No Warning with NAs
  negatives <- data.frame(rr = c(1.0, NA), seLogRr = c(1.0, 2.0))

  result <- computeCalibratedRows(positives = positives,
                                  negatives = negatives,
                                  keepCols = NULL)

  checkmate::expect_data_frame(result,
                               nrows = nrow(positives),
                               col.names = "named")

  checkmate::expect_names(names(result), must.include = c("calibrated", "pValue", "ub95", "lb95", "rr", "seLogRr"))

  # Error without negatives
  expect_error(computeCalibratedRows(positives = positives,
                                     negatives = NULL,
                                     keepCols = NULL))

  testNull <- createNullDist(1.0, 2.0)
  checkmate::expect_class(testNull, "null")
  checkmate::expect_names(names(testNull), must.include = c("mean", "sd"))

  result <- computeCalibratedRows(positives = positives,
                                  nullDist = testNull,
                                  keepCols = NULL)

  checkmate::expect_data_frame(result,
                               nrows = nrow(positives),
                               col.names = "named")

  checkmate::expect_names(names(result), must.include = c("calibrated", "pValue", "ub95", "lb95", "rr", "seLogRr"))


  positives <- data.frame(rr = c(1.0, 2.0, 1.5),
                          cPt = c(5, 6, 7),
                          myId = 1211,
                          cAtRisk = c(8, 9, 10),
                          tCases = c(11, 12, 13),
                          cCases = c(11, 12, 13),
                          tAtRisk = c(14, 15, 16),
                          seLogRr = c(1.0, 1.2, 2.5))
  negatives <- data.frame(rr = c(1.0), seLogRr = c(1.0))

  # Test that getting cols works
  result <- computeCalibratedRows(positives = positives,
                                  negatives = negatives,
                                  idCol = "myId")

  reqCols <- c("cPt", "cAtRisk", "cCases", "tCases", "tAtRisk")

  checkmate::expect_data_frame(result,
                               nrows = nrow(positives),
                               col.names = "named")

  checkmate::expect_names(names(result), must.include = c(reqCols,
                                                          "calibrated", "pValue", "ub95", "lb95", "rr", "seLogRr", "myId"))
})