
#' Select a row from a data frame based on weighted sampling of a specified column,
#' with priority given to a specific value.
#'
#' @param places Data frame containing the data to sample from
#' @param constrain_name Character string; name of the column to base sampling weights on
#' @param constrain_priority Character string; value in the constrain_name column to prioritize
#' @param constrain_options Character vector; all possible values in the constrain_name column
#' @param constrain_priority_weight Numeric; weight for the priority value (0-1). Defaults to 0.85
#' @param check_constrain_priority Logical; if TRUE, errors if constrain_priority isn't in
#'   constrain_options. Defaults to FALSE
#' @return A single-row data frame sampled based on weighted probabilities
#' @details The remaining weight (1 - constrain_priority_weight) is distributed equally
#'   among other values in constrain_options. If constrain_priority isn't in constrain_options
#'   and check_constrain_priority is FALSE, returns a random sample.
#' @examples
#'   df <- data.frame(category = c("x", "y", "z"), value = 1:3)
#'   select_place_with_constrain(df, "category", "y", c("x", "y", "z"))
#
select_place_with_constrain <- function(
  places,
  constrain_name,
  constrain_priority,
  constrain_options,
  constrain_priority_weight = 0.85,
  check_constrain_priority = FALSE
){
    # Check if constrain_name exists in the data frame
    if (!constrain_name %in% names(places)) {
        stop(sprintf("The constrain name: %s is not in the place_data", constrain_name))
    }

    # Check if constrain_priority is in constrain_options
    if (!constrain_priority %in% constrain_options) {
        if (check_constrain_priority) {
        stop(sprintf(
            "The agent constrain priority %s (%s) is not part of the provided constrain_options",
            constrain_priority, constrain_name
        ))
        } else {
        return(places[sample(nrow(places), 1), , drop = FALSE])
        }
    }

    # Calculate weights for non-priority options
    constrain_weights_others <- (1.0 - constrain_priority_weight) / (length(constrain_options) - 1)
    
    # Create vector of weights, initially all equal for non-priority
    constrain_weights <- rep(constrain_weights_others, length(constrain_options))
    
    # Set priority weight
    priority_index <- which(constrain_options == constrain_priority)
    constrain_weights[priority_index] <- constrain_priority_weight
    
    # Create named vector of weights
    names(constrain_weights) <- constrain_options
    
    # Map weights to the data frame rows
    df_weights <- constrain_weights[places[[constrain_name]]]
    
    # Sample one row with weights
    sampled_row <- places[sample(nrow(places), 1, prob = df_weights), , drop = FALSE]
    
    return(sampled_row)
}