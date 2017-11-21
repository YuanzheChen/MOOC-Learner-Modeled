import getpass
import time

import MySQLdb
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
from sklearn import preprocessing, linear_model
from sklearn.metrics import confusion_matrix
from sklearn.model_selection import train_test_split


def dropout_structure(date_string):
    conn = MySQLdb.connect(host='localhost', user='alhuang', passwd=getpass.getpass(),
                           db='201x_2013_Spring_fullpipe_clone')
    allFeaturesDF = pd.read_sql(
        sql="select * from user_longitudinal_feature_values where date_of_extraction > '" + date_string + "'",
        con=conn)

    users = allFeaturesDF['user_id'].unique()

    count = 0

    dropout_week_count = [0] * 16

    for user in users:

        # print user

        individualDF = allFeaturesDF.loc[allFeaturesDF['user_id'] == user]
        individualDF = individualDF.loc[individualDF['longitudinal_feature_id'] == 1]

        dropouts = individualDF['longitudinal_feature_value'].tolist()

        last_week_in_class = None
        dropout_week = None

        # print dropouts

        try:
            last_week_in_class = max(loc for loc, val in enumerate(dropouts) if val == 1)

        except:
            last_week_in_class = -1

        try:
            dropout_week = min(loc for loc, val in enumerate(dropouts) if val == 0)

        except:
            dropout_week = 15

        if last_week_in_class is not None and dropout_week is not None:
            assert last_week_in_class < dropout_week

        dropout_week_count[dropout_week] = dropout_week_count[dropout_week] + 1

        if count % 500 == 0:
            print count
        count += 1

    plt.clf()
    ax = plt.grid()
    x = np.arange(0, len(dropout_week_count))
    plt.bar(x, dropout_week_count)
    plt.title("Dropout Counts Each Week (15 = Completion)")
    plt.xlabel("Week")
    plt.ylabel("Dropout Counts")
    plt.savefig("Dropouts_by_Week.png")
    plt.clf()

    return dropout_week_count


def plot_weekly_feature_counts(features_to_examine, feature_counts, week_num):
    print "Generating", week_num

    plt.clf()
    ax = plt.grid()
    x = np.arange(1, len(features_to_examine) + 1)
    plt.bar(x, feature_counts, align='center')
    plt.xticks(x, features_to_examine, rotation=45)
    plt.title('Week ' + str(week_num))
    plt.xlabel('Feature Number')
    plt.ylabel('Count of Users with Feature')
    plt.savefig('Feature_Counts/Week ' + str(week_num) + ' Feature Counts')
    plt.clf()


def plot_user_feature_amounts(user_feature_amounts, week_num):
    print "Generating", week_num

    plt.clf()
    ax = plt.grid()
    x = np.arange(len(user_feature_amounts))
    plt.bar(x, user_feature_amounts, align='center')
    plt.xticks(x, features_to_examine, rotation=45)
    plt.title('Week ' + str(week_num) + "- How Many Users Have X Number of Features")
    plt.xlabel('Feature Count')
    plt.ylabel('User Count With Number of Features')
    plt.savefig('User_Feature_Counts/Week ' + str(week_num) + ' Feature Counts')
    plt.clf()


def plot_student_distribution(num_weeks, staying_count, dropout_count):
    width = .5
    ind = np.array(range(num_weeks))

    ax = plt.subplot(111)
    rects1 = ax.bar(ind, staying_count, width, color='g')
    rects2 = ax.bar(ind + width, dropout_count, width, color='r')
    ax.set_title('Class Distribution Over Time')
    ax.set_xlabel('Weeks')
    ax.set_ylabel('Student Count')
    plt.savefig("Distribution Over Time.png")


def collect_data(extraction_date):
    print "Training on all previous week data:", train_on_all_previous
    print "Features used:", features_to_examine
    # connection = MySQLdb.connect(host='alfad4.csail.mit.edu', user='alex_huang', passwd='ning1023', db='201x_2013_spring')
    # moocConnection = MySQLdb.connect(host='localhost', user='alhuang', passwd=getpass.getPassword(), db='201x_2013_Spring_fullpipe_clone')
    moocConnection = MySQLdb.connect(host='localhost', user='alhuang', passwd=getpass.getpass(),
                                     db='201x_2013_Spring_fullpipe_clone')
    allFeaturesDF = pd.read_sql(
        sql="select * from user_longitudinal_feature_values where date_of_extraction > '" + extraction_date + "'",
        con=moocConnection)

    return

    # Incorporating all data
    all_user_features = []
    all_user_dropouts = []

    total_correct = 0.0
    total_incorrect = 0.0

    num_weeks = max(allFeaturesDF['longitudinal_feature_week'].unique())

    user_feature_counts_overall = []

    staying_counts = []
    dropout_counts = []

    all_feature_counts = []

    # for week_num in range(1, num_weeks+1): # ignore trivial first week
    for week_num in allFeaturesDF['longitudinal_feature_week'].unique():

        print week_num

        week_data = allFeaturesDF.loc[allFeaturesDF['longitudinal_feature_week'] == week_num]
        week_users = week_data['user_id'].unique()

        ### Counting features
        feature_counts = []

        for f in features_to_examine:
            feature_counts.append(len(week_data.loc[week_data['longitudinal_feature_id'] == f]))

        all_feature_counts.append(feature_counts)
        ### End of feature counting




        user_feature_counts = [0] * (len(features_to_examine) + 1)

        user_features = []
        user_dropouts = []

        count = 0

        for user in week_users:

            user_week_data = week_data.loc[week_data['user_id'] == user]

            # Get the value for each of the examined features into a vector, then append that vector
            user_feature_vector = []

            for feature in features_to_examine:
                feature_value = 0  # default value for if the feature is not available

                if feature in user_week_data['longitudinal_feature_id'].values:
                    feature_value = user_week_data.loc[user_week_data['longitudinal_feature_id'] == feature][
                        'longitudinal_feature_value'].values[0]

                    # Only append if we see
                    user_feature_vector.append(feature_value)


                    # user_feature_vector.append(feature_value)

            # Don't append if all the values are 0
            if all(value == 0 for value in user_feature_vector):
                continue

            user_features.append(user_feature_vector)
            # Get the dropout value for the user
            dropout_value = user_week_data.loc[user_week_data['longitudinal_feature_id'] == DROPOUT_FEATURE_ID_VALUE][
                'longitudinal_feature_value'].values[0]
            user_dropouts.append(dropout_value)

            user_feature_count = len(user_feature_vector)
            user_feature_counts[user_feature_count] = user_feature_counts[user_feature_count] + 1

        # print user_feature_counts


        dropout_student_count = user_dropouts.count(0)
        current_students_count = user_dropouts.count(1)

        staying_counts.append(current_students_count)
        dropout_counts.append(dropout_student_count)

        user_feature_counts_overall.append(user_feature_counts)

    # return user_feature_counts_overall

    # Counting Features
    return all_feature_counts


DROPOUT_FEATURE_ID_VALUE = 1
features_to_examine = [2, 3, 6, 7, 8, 15, 16, 208, 209, 210, 400, 401, 402, 403]
train_on_all_previous = False
print_weekly_information = True
normalize_data = True


def train_model(extraction_date, user, passwd, db_name):
    print "Training on all previous week data:", train_on_all_previous
    print "Features used:", features_to_examine

    mooc_connection = MySQLdb.connect(host='localhost', user=user, passwd=passwd, db=db_name)
    all_features_df = pd.read_sql(
        sql="select * from user_longitudinal_feature_values where date_of_extraction > '" + extraction_date + "'",
        con=mooc_connection)

    # Incorporating all data
    all_user_features = []
    all_user_dropouts = []

    total_correct = 0.0
    total_incorrect = 0.0

    num_weeks = max(all_features_df['longitudinal_feature_week'].unique()) + 1

    weeks_values = []
    stay_in_and_dropout_values = []

    weekly_count = []
    remain_count = []
    dropout_count = []


    for week_num in range(1, num_weeks):  # ignore trivial first week
        # for week_num in allFeaturesDF['longitudinal_feature_week'].unique():
        print week_num

        week_data = all_features_df.loc[all_features_df['longitudinal_feature_week'] == week_num]
        week_users = week_data['user_id'].unique()

        # Each index of the features and dropouts array correspond to a given user
        user_features = []
        user_dropouts = []

        count = 0

        # Iterate through each user
        print("Number of users in week", len(week_users))

        user_num_distinct_features_counts = [0] * len(features_to_examine)

        dropout_feature_counts_sum = 0.0
        stayin_feature_counts_sum = 0.0

        for i, user in enumerate(week_users):

            # Track program progress
            if i % 2500 == 0:
                print(i)

            user_week_data = week_data.loc[week_data['user_id'] == user]

            #			if len(user_week_data) < 9:
            #				continue

            # Get the value for each of the examined features into a vector, then append that vector
            user_feature_vector = []

            for feature in features_to_examine:
                feature_value = 0  # default value for if the feature is not available

                existing_feature_values = user_week_data['longitudinal_feature_id'].values
                # TODO optimize
                if feature in existing_feature_values:
                    feature_value = user_week_data.loc[user_week_data['longitudinal_feature_id'] == feature][
                        'longitudinal_feature_value'].values[0]

                user_feature_vector.append(feature_value)

            # Don't append if all the values are 0
            number_of_feature_values = np.count_nonzero(np.array(user_feature_vector))
            user_num_distinct_features_counts[number_of_feature_values] += 1
            #			if number_of_features_values < 7:
            #				continue

            #			print(user_feature_vector)

            user_features.append(user_feature_vector)
            # Get the dropout value for the user
            dropout_value = user_week_data.loc[user_week_data['longitudinal_feature_id'] == DROPOUT_FEATURE_ID_VALUE][
                'longitudinal_feature_value'].values[0]
            # print(dropout_value, number_of_feature_values)

            # Dropout/Non-dropout average feature counts
            if dropout_value == 0:
                dropout_feature_counts_sum += number_of_feature_values
            else:
                stayin_feature_counts_sum += number_of_feature_values

            user_dropouts.append(dropout_value)

        print("Distribution of feature counts:", user_num_distinct_features_counts)

        # user_features contains a feature vector for each user
        # user_dropout contains the associated user vectors

        # Add to the cumulative tracker
        total_count = len(user_dropouts)

        dropout_student_count = user_dropouts.count(0)
        current_students_count = user_dropouts.count(1)

        print("Dropout, Current:", dropout_student_count,
              current_students_count)

        print("Average dropout feature count:",
              dropout_feature_counts_sum / dropout_student_count)
        print("Average stayin feature count:",
              stayin_feature_counts_sum / current_students_count)

        weekly_count.append(total_count)
        remain_count.append(current_students_count)
        dropout_count.append(dropout_student_count)

        # Normalize the feature data before training
        if normalize_data:
            user_features = np.array(user_features)
            min_max_scaler = preprocessing.MinMaxScaler()
            user_features = min_max_scaler.fit_transform(user_features)

        all_user_features.extend(user_features)
        all_user_dropouts.extend(user_dropouts)

        # Perform the weekly training

        if all(v == 1 for v in user_dropouts):
            print("No dropouts - not performing analysis")
            continue

        feature_train, feature_test, dropout_train, dropout_test = train_test_split(user_features, user_dropouts,
                                                                                    test_size=.9)

        logisticRegressionClassifier = linear_model.LogisticRegression()

        # fFit the data on the training data from this week
        logisticRegressionClassifier.fit(feature_train, dropout_train)

        # Option to training on all previous data
        if train_on_all_previous:
            if len(all_user_features) != 0:
                logisticRegressionClassifier.fit(all_user_features, all_user_dropouts)
            all_user_features.extend(user_features)
            all_user_dropouts.extend(user_dropouts)

        # Document distribution of droppouts vs retentions
        dropout_student_count = user_dropouts.count(0)
        current_students_count = user_dropouts.count(1)
        test_set_dropout_student_count = dropout_test.count(0)
        test_set_current_students_count = dropout_test.count(1)

        weeks_values.append(week_num)
        stay_in_and_dropout_values.append((current_students_count, dropout_student_count))

        # After training the classifier, make a prediction using the test data set, feature_set
        test_set_prediction = logisticRegressionClassifier.predict(feature_test)

        # Construct a confusion matrix by comparing what we predict to the actual dropout values for our test set, dropout_test
        week_data_confusion_matrix = confusion_matrix(dropout_test, test_set_prediction)

        # Keep track of overall correctness percentage
        test_percent_correct = logisticRegressionClassifier.score(feature_test, dropout_test)
        num_correct = len(feature_test) * test_percent_correct
        num_incorrect = len(feature_test) - num_correct
        total_correct += num_correct
        total_incorrect += num_incorrect

        if print_weekly_information:
            print "Week", week_num
            print "Number of data points this week -", len(user_features), "Training Points:", len(
                feature_train), "Testing Points:", len(feature_test)
            print "Week", week_num, "Accuracy on", len(
                feature_test), "points -", test_percent_correct, num_correct, num_incorrect
            print "Current students:", current_students_count, "Dropout students:", dropout_student_count
            print "Test set current students:", test_set_current_students_count, "Test set dropout students:", test_set_dropout_student_count
            print week_data_confusion_matrix
            # print "Above, the count of true negatives is C[0,0], false negatives is C[1,0], true positives is C[1,1], and false positives is C[0,1]."
            print ""

    return weekly_count, dropout_count, remain_count

    print "Features used:", features_to_examine
    print "Total accuracy:", total_correct / (total_correct + total_incorrect)

    return weeks_values, stay_in_and_dropout_values


start_time = time.clock()
end_time = time.clock()