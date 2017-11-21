import MySQLdb
import sys
import pandas

from sklearn.model_selection import train_test_split
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import confusion_matrix
from sklearn import preprocessing
from sklearn.metrics import roc_curve, auc, roc_auc_score

# Models:
from sklearn.linear_model import LogisticRegression
from sklearn.svm import SVC
from sklearn.gaussian_process import GaussianProcessClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.neural_network import MLPClassifier


import time
import matplotlib
matplotlib.use('Agg')

from matplotlib import pyplot as plt
import numpy as np
from collections import defaultdict
import pickle

DROPOUT_FEATURE_ID = 1
MISSING_FEATURE_VALUE = -1

# To use if the feature_id is missing
# Features
# 2 - sum_observed_events_duration
# 6 - distinct question attempts
# 7 - total number of question attempts
# 8 - distinct problems correct
# 15 - max_duration_resources (duration of longest observed event)
# 16 - sum of observed events (lectures)
# 208 - correct attempts
# 209 - percent correct submissions
# 210 - average predeadline submission time
# 400 - number of threads involved in
# 401 - number of replies
# 402 - number of threads started
# 403 - number of replies received


def train_model(extraction_date, db_name, features_to_use, model_to_use,
                feature_vector_dict_file_name=None,
                dropout_vector_dict_file_name=None,
                generate_ROC_curves=True,
                last_week_to_perform_analysis=10,
                weeks_ahead_to_predict=1,
                collect_data=False,
                minimum_feature_support=3,
                normalize_data=True,
                test_size_proportion=0.25,
                verbose=True,
                train_on_all_data=False,
                ):

    print("Verbose:", verbose)
    print("Database: " + db_name)
    print("Min Feature Support: {}".format(minimum_feature_support))
    print("Weeks Ahead To Predict: {}".format(weeks_ahead_to_predict))
    print("Model: {}".format(model_to_use))

    mooc_connection = MySQLdb.connect(host='localhost', user='vismooc', passwd='vismooc', db=db_name)

    all_features_df = pandas.read_sql(
        sql="select * from user_long_feature where date_of_extraction > '" + extraction_date + "'",
        con=mooc_connection)

    users = all_features_df['user_id'].unique()

    num_weeks = max(all_features_df.loc[all_features_df['feature_id'] == 1]['feature_week'].unique())

    print("Highest Week Number with Dropout: {}".format(num_weeks))

    data_by_week = {}

    user_feature_vector_dictionary = defaultdict(dict) # maps users to a dictionary that maps weeks to the user's vector for that week
    user_dropouts_dictionary = defaultdict(list) # maps users to a list that indicates their dropout values, index of list corresponds to week

    # Generate the data if you have not previously generated
    if collect_data:

        print("Collecting and Saving Data")
        for week in range(num_weeks+1):

            current_week_df = all_features_df.loc[all_features_df['feature_week'] == week]

            print("Week:", week)

            for i, user_id in enumerate(users):

                user_feature_vector = get_user_feature_values_week(user_id, week, current_week_df, features_to_use)
                user_feature_vector_dictionary[user_id][week] = user_feature_vector

                user_dropout = get_user_dropout_by_week(user_id, week, current_week_df)
                user_dropouts_dictionary[user_id].append(user_dropout)

        print("Saving pickle files")
        # Dump into pickle files if they don't exist already
        pickle.dump(user_feature_vector_dictionary, open("./"+db_name+"_user_feature_vector_dictionary.p", "wb"))
        pickle.dump(user_dropouts_dictionary, open("./"+db_name+"_user_dropouts_dictionary.p", "wb"))

    else:
        user_feature_vector_dictionary = pickle.load(open("./"+feature_vector_dict_file_name, "rb"))
        user_dropouts_dictionary = pickle.load(open("./"+dropout_vector_dict_file_name, "rb"))

    output_file = open("./" + db_name + model_to_use + "_" + str(weeks_ahead_to_predict)+"_weeks_ahead_results_" + str(minimum_feature_support) + "_minimum_feature_support.txt", "w")

    # Plotting and data collection
    total_correct = 0.0
    total_incorrect = 0.0

    percent_correct_record = []
    all_aucs = []

    # Map week to classifier
    classifiers = {}

    features_to_counts = defaultdict(int)
    total_feature_count = []

    # Get dropout data beforehand
    for week in range(min(num_weeks - weeks_ahead_to_predict + 1, last_week_to_perform_analysis+1)): # Go up to week 11

        weekly_features_to_counts = defaultdict(int)
        weekly_total_feature_count = []

        user_feature_vectors = []
        user_dropouts = []

        current_week = week
        ahead_week = week + weeks_ahead_to_predict

        forum_feature_count = 0.0

        for i, user_id in enumerate(users):

            # if i % 2000 == 0:
            #     print(i)

            user_feature_vector = user_feature_vector_dictionary[user_id][current_week] # Current week for features
            number_of_present_features = len(user_feature_vector) - user_feature_vector.count(-1)

            if number_of_present_features > minimum_feature_support:

                # Check average features per user and counts for each feature
                total_feature_count.append(number_of_present_features)
                weekly_total_feature_count.append(number_of_present_features)

                for j, feature_num in enumerate(features_to_use):
                    if user_feature_vector[j] != -1:
                        features_to_counts[feature_num] += 1
                        weekly_features_to_counts[feature_num] += 1

                user_feature_vectors.append(user_feature_vector)

                user_dropout = user_dropouts_dictionary[user_id][ahead_week] # Use the ahead week to get the dropout
                user_dropouts.append(user_dropout)

        # print("Week:", week, "Num of Users With Forum Feature Count:", forum_feature_count, "Total Users:", len(user_feature_vectors))

        # print("Week:", week, "Number of dropouts", user_dropouts.count(0), "Number of Stay-ins", user_dropouts.count(1), "Total", len(user_dropouts))
        #
        # continue

        if len(user_feature_vectors) < 10:
            print("Week {} - Not enough users with required support".format(week))
            continue

        # Normalize the feature data before training
        if normalize_data:
            user_feature_vectors = np.array(user_feature_vectors)
            min_max_scaler = preprocessing.MinMaxScaler()
            user_feature_vectors = min_max_scaler.fit_transform(user_feature_vectors)

        data_by_week[week] = (user_feature_vectors, user_dropouts)

        if float(user_dropouts.count(1)) > float(len(user_dropouts)) * 0.9:
            print("All dropouts - not performing analysis")
            continue

        classifier = None

        if model_to_use == 'logistic_regression':
            classifier = GridSearchCV(LogisticRegression(), param_grid = [{'C':[1, 10, 100, 1000, 10000],'fit_intercept': [True, False],  'solver':['newton-cg', 'lbfgs', 'liblinear', 'sag']}])
        elif model_to_use == 'svm':
            classifier = GridSearchCV(SVC(), param_grid = [{'C':[1, 10, 100, 1000], 'kernel':['linear', 'poly', 'rbf'], 'degree':[3,4,5], 'probability':[True, False], 'shrinking':[True, False]}])
        elif model_to_use == 'gaussian_process':
            classifier = GridSearchCV(GaussianProcessClassifier(), param_grid = [{'optimizer':[None, 'fmin_l_bfgs_b'], 'n_restarts_optimizer':[0,1,2]}])
        elif model_to_use == 'decision_tree':
            classifier = GridSearchCV(DecisionTreeClassifier(), param_grid = [{'criterion':['gini', 'entropy'], 'max_features':['auto', 'sqrt', 'log2', None]}])
        elif model_to_use == 'random_forest':
            classifier = GridSearchCV(RandomForestClassifier(), param_grid = [{'n_estimators':[10, 20, 30], 'criterion':['gini', 'entropy'], 'max_features':['sqrt', 'log2', None], 'bootstrap':[True, False]}])
        elif model_to_use == 'neural_network':
            classifier = GridSearchCV(MLPClassifier(), param_grid = [{'activation':['identity', 'logistic', 'tanh', 'relu'], 'solver':['sgd', 'adam'], 'learning_rate':['constant', 'invscaling', 'adaptive'], 'shuffle':[True, False]}])
        else:
            print("Invalid Model Specification")

        if train_on_all_data:

            classifier.fit(user_feature_vectors, user_dropouts)
            classifiers[week] = classifier
            # Don't test on hold out if just trying to get classifier. move on to next week
            continue

        else: # For training/testing on just a single class
            feature_train, feature_test, dropout_train, dropout_test = train_test_split(user_feature_vectors, user_dropouts,
                                                                                        test_size=test_size_proportion, random_state=19)
            classifier.fit(feature_train, dropout_train)
            classifiers[week] = classifier


        # ROC curve generation
        if generate_ROC_curves:

            dropout_confidence_scores = None
            if model_to_use == 'svm':
                dropout_confidence_scores = classifier.decision_function(feature_test)
            else:
                # Get positive class estimate, can also use decision_function but not present in all class models (GP's)
                dropout_confidence_scores = [x[1] for x in classifier.predict_proba(feature_test)]



            fpr, tpr, threshold = roc_curve(dropout_test, dropout_confidence_scores)
            try:
                roc_auc = roc_auc_score(dropout_test, dropout_confidence_scores)
                all_aucs.append(roc_auc)
            except ValueError as e:
                print(e)
                roc_auc = -1

                # Generate ROC plot
            # plt.figure()
            lw = 2
            plt.plot(fpr, tpr, color='darkorange',
                     lw=lw, label='ROC curve (area = %0.2f)' % roc_auc)
            plt.plot([0, 1], [0, 1], color='navy', lw=lw, linestyle='--')
            plt.xlim([0.0, 1.0])
            plt.ylim([0.0, 1.05])
            plt.xlabel('False Positive Rate')
            plt.ylabel('True Positive Rate')
            plt.title('Week {} ROC'.format(week))
            plt.legend(loc="lower right")
            plt.savefig("./" + db_name + '_' + model_to_use + "_Week_" + str(week) + "_ROC.png")
            plt.clf()




        test_set_prediction = classifier.predict(feature_test)
        week_confusion_matrix = confusion_matrix(dropout_test, test_set_prediction)
        test_percent_correct = classifier.score(feature_test, dropout_test)

        # Plotting and data collection
        total_correct += len(dropout_train)*test_percent_correct
        total_incorrect += len(dropout_train)*(1-test_percent_correct)


        # Write results to file
        output_file.write("Training Week:" + str(current_week) + " Prediction Week: " + str(ahead_week) + "\n")
        output_file.write("Percentage Correct: " + str(test_percent_correct) + "\n")
        output_file.write("AUC:" + str(roc_auc) + "\n")
        percent_correct_record.append(test_percent_correct)

        output_file.write("Total Users With Min Support: " + str(len(user_feature_vectors)) + "\n")
        output_file.write("Users in Test Set: " + str(len(dropout_train)) + "\n")
        output_file.write("Test Correct: " + str(len(dropout_train)*test_percent_correct) + "\n")
        output_file.write("Test Incorrect: " + str(len(dropout_train)*(1-test_percent_correct)) + "\n")
        output_file.write("Confusion Matrix" + "\n")
        # output_file.write(str(week_confusion_matrix[0][0]) + " " + str(week_confusion_matrix[0][1]) + "\n")
        # output_file.write(str(week_confusion_matrix[1][0]) + " " + str(week_confusion_matrix[1][1]) + "\n")

        output_file.write("Total Number of Occurrences for Each Feature:".format(week) + "\n")
        for k, v in sorted(weekly_features_to_counts.items()):
            output_file.write(str(k))
            output_file.write(": ")
            output_file.write(str(v))
            output_file.write("\n")

        output_file.write("Average Feature Cardinality: " + str(sum(weekly_total_feature_count) / len(weekly_total_feature_count)) + "\n")
        output_file.write("\n\n")

        if verbose:
            print("Training Week:", current_week)
            print("Prediction Week:", ahead_week)
            print("Percentage Correct:", test_percent_correct)
            print("Total Users With Min Support:", len(user_feature_vectors), "--", "Correct:", len(user_feature_vectors)*test_percent_correct,
                  "Incorrect:", len(user_feature_vectors)*(1-test_percent_correct))
            print("Confusion Matrix:")
            print(week_confusion_matrix)

    if train_on_all_data:
        return classifiers

    output_file.write("\n\n")
    output_file.write("Total Correct: " + str(total_correct) + "\n")
    output_file.write("Total Incorrect: " + str(total_incorrect) + "\n")

    output_file.write("Total Number of Occurrences for Each Feature:\n")
    for k, v in features_to_counts.items():
        output_file.write(str(k))
        output_file.write(": ")
        output_file.write(str(v))
        output_file.write("\n")

    output_file.write("Average Feature Cardinality: " + str(sum(total_feature_count)/len(total_feature_count)))
    output_file.close()


    # return total_correct/(total_correct+total_incorrect)  # For model selection scripts
    # return data_by_week

    # Return the generated classifier for each week
    return classifiers

def score_ensemble_classifiers(classifiers_1,
                               classifiers_2,
                               feature_vector_dict_file_name=None,
                               dropout_vector_dict_file_name=None,
                               weeks_ahead_to_predict=1,
                               minimum_feature_support=3,
                               normalize_data=True,
                               ):

    print("Feature Vector File:", feature_vector_dict_file_name)

    user_feature_vector_dictionary = pickle.load(open(feature_vector_dict_file_name, "rb"))
    user_dropouts_dictionary = pickle.load(open(dropout_vector_dict_file_name, "rb"))

    users = user_feature_vector_dictionary.keys()

    total_correct = 0.0
    total_incorrect = 0.0


    for week in range(3, 11):

        classifier_1 = classifiers_1[week]
        classifier_2 = classifiers_2[week]

        print("Testing Week {}".format(week))

        user_feature_vectors = []
        user_dropouts = []

        current_week = week
        ahead_week = week + weeks_ahead_to_predict

        for i, user_id in enumerate(users):

            user_feature_vector = user_feature_vector_dictionary[user_id][current_week]  # Current week for features
            number_of_present_features = len(user_feature_vector) - user_feature_vector.count(-1)

            if number_of_present_features > minimum_feature_support:

                user_feature_vectors.append(user_feature_vector)
                user_dropout = user_dropouts_dictionary[user_id][ahead_week]  # Use the ahead week to get the dropout
                user_dropouts.append(user_dropout)

        if len(user_feature_vectors) < 10:
            print("Week {} - Not enough users with required support".format(week))
            continue

        if normalize_data:
            user_feature_vectors = np.array(user_feature_vectors)
            min_max_scaler = preprocessing.MinMaxScaler()
            user_feature_vectors = min_max_scaler.fit_transform(user_feature_vectors)

        probabilities_1 = classifier_1.predict_proba(user_feature_vectors)
        probabilities_2 = classifier_2.predict_proba(user_feature_vectors)

        average_probabilities = (probabilities_1 + probabilities_2) / 2

        average_prob_predict_1 = average_probabilities[:, 1]

        predictions = [1 if x>0.5 else 0 for x in average_prob_predict_1]

        correct = 0.0
        incorrect = 0.0

        for i,p in enumerate(predictions):
            if p == user_dropouts[i]:
                correct+=1
            else:
                incorrect+=1
        print(correct / (correct+incorrect))

        total_correct += correct
        total_incorrect += incorrect

    print("\nOverall:", total_correct / (total_correct+total_incorrect))


def score_classifier(classifiers,
                     feature_vector_dict_file_name=None,
                     dropout_vector_dict_file_name=None,
                     weeks_ahead_to_predict=1,
                     minimum_feature_support=3,
                     normalize_data=True,
                     verbose=True
                     ):

    print("Feature Vector File:", feature_vector_dict_file_name)

    user_feature_vector_dictionary = pickle.load(open(feature_vector_dict_file_name, "rb"))
    user_dropouts_dictionary = pickle.load(open(dropout_vector_dict_file_name, "rb"))

    users = user_feature_vector_dictionary.keys()

    total_correct = 0.0
    total_incorrect = 0.0


    for week in classifiers.keys():

        classifier = classifiers[week]

        print("Testing Week {}".format(week))

        user_feature_vectors = []
        user_dropouts = []

        current_week = week
        ahead_week = week + weeks_ahead_to_predict

        for i, user_id in enumerate(users):

            user_feature_vector = user_feature_vector_dictionary[user_id][current_week]  # Current week for features
            number_of_present_features = len(user_feature_vector) - user_feature_vector.count(-1)

            if number_of_present_features > minimum_feature_support:

                user_feature_vectors.append(user_feature_vector)
                user_dropout = user_dropouts_dictionary[user_id][ahead_week]  # Use the ahead week to get the dropout
                user_dropouts.append(user_dropout)

        if len(user_feature_vectors) < 10:
            print("Week {} - Not enough users with required support".format(week))
            continue

        if normalize_data:
            user_feature_vectors = np.array(user_feature_vectors)
            min_max_scaler = preprocessing.MinMaxScaler()
            user_feature_vectors = min_max_scaler.fit_transform(user_feature_vectors)

        predictions = classifier.predict(user_feature_vectors)
        computed_confusion_matrix = confusion_matrix(user_dropouts, predictions)
        test_percent_correct = classifier.score(user_feature_vectors, user_dropouts)

        total_correct += len(user_feature_vectors)*test_percent_correct
        total_incorrect += len(user_feature_vectors)*(1-test_percent_correct)

        if verbose:
            print("Training Week:", current_week)
            print("Prediction Week:", ahead_week)
            print("Percentage Correct:", test_percent_correct)
            # print("Total Users With Min Support:", len(user_feature_vectors), "--", "Correct:",
            #       len(user_feature_vectors) * test_percent_correct,
            #       "Incorrect:", len(user_feature_vectors) * (1 - test_percent_correct))
            print("Confusion Matrix:")
            print(computed_confusion_matrix)

    print("\n\n")
    print("Total Percentage:", total_correct/(total_correct+total_incorrect))


def train_transfer_learning():

    print("HERE")

    extraction_date = "2017-05-09 00:00:00"
    db_name = "carinaq_sample"
    feature_vector_dict_file = "carinaq_sample_user_feature_vector_dictionary.p"
    dropout_vector_dict_file = "carinaq_sample_user_dropouts.p"



    features_to_use = [2, 3, 6, 7, 8, 15, 16, 208, 209, 210, 400, 401, 402, 403]



    classifiers = train_model(extraction_date, db_name, features_to_use, model,
                              feature_vector_dict_file_name=feature_vector_dict_file,
                              dropout_vector_dict_file_name=dropout_vector_dict_file,
                              verbose=True, collect_data=True,
                              train_on_all_data=True)


# Function to get the feature values for a given user_id and week
def get_user_feature_values_week(user_id, week, features_df, features_to_use):

    # test user - '01142f938fc7743d423965b3a35fdaf92646cf65', week 5
    # Get the data for the given week and user
    user_week_feat_values_df = features_df.loc[(features_df['user_id'] == user_id)
                                               & (features_df['feature_week'] == week)]
    user_values_dict = dict(zip(user_week_feat_values_df.feature_id,
                                user_week_feat_values_df.feature_value))

    output_feature_vector = [user_values_dict[feature] if feature in user_values_dict else MISSING_FEATURE_VALUE for feature in features_to_use]

    return output_feature_vector

# dropout feature number is 1
def get_user_dropout_by_week(user_id, week, features_df):

    dropout_value = features_df.loc[(features_df['user_id'] == user_id)
                                    & (features_df['feature_week'] == week)
                                    & (features_df['feature_id'] == DROPOUT_FEATURE_ID)]['feature_value'].iloc[0]

    return dropout_value


def run_all_models():

    models = []

    models.append("logistic_regression")
    models.append("svm")
    # models.append("gaussian_process")
    models.append("decision_tree")
    models.append("random_forest")
    models.append("neural_network")

    print(models)

    # extraction_date = "2017-04-10 00:00:00"
    # db_name = "alhuang_mitx_2400x_2013_sond"
    # features_to_use = [2, 3, 6, 7, 8, 15, 16, 208, 209, 210, 400, 401, 402, 403]
    # feature_vector_dict_file = "2400x_2013_sond_user_feature_vector_dictionary.p"
    # dropout_vector_dict_file = "2400x_2013_sond_user_dropouts.p"

    # *** 3.086 Parameters ***
    # extraction_date = "2017-05-09 00:00:00"
    # db_name = "alhuang_mitx_3086x_2013_sond"
    # features_to_use = [2, 6, 7, 8, 15, 208, 209, 210, 400, 401, 402, 403] #Missing 3, 16
    # feature_vector_dict_file = "alhuang_mitx_3086x_2013_sond_user_feature_vector_dictionary.p"
    # dropout_vector_dict_file = "alhuang_mitx_3086x_2013_sond_user_dropouts_dictionary.p"

    # *** 6.002 Parameters ***
    # extraction_date = "2017-05-09 00:00:00"
    # db_name = "alhuang_mitx_6002x_2012_fall"
    # features_to_use = [2, 6, 7, 8, 15, 16, 208, 209, 400, 401, 402, 403] #Missing 3, 8, 208
    # feature_vector_dict_file = "alhuang_mitx_6002x_2012_fall_user_feature_vector_dictionary.p"
    # dropout_vector_dict_file = "alhuang_mitx_6002x_2012_fall_user_dropouts_dictionary.p"

    for model in models:
        train_model(extraction_date, db_name, features_to_use, model,
                    feature_vector_dict_file_name=feature_vector_dict_file,
                    dropout_vector_dict_file_name=dropout_vector_dict_file,
                    verbose=False)


if __name__ == '__main__':
    extraction_date = "2017-05-09 00:00:00"
    db_name = "moocdb"
    features_to_use = [2, 6, 7, 8, 9, 10, 11, 12, 13]
    feature_vector_dict_file = "user_feature_vector_dictionary.p"
    dropout_vector_dict_file = "user_dropouts.p"
    model = "svm"
    print("Features To Use,", features_to_use)
    start = time.clock()

    classifiers = train_model(extraction_date, db_name, features_to_use, model,
                              minimum_feature_support=-1, collect_data=True,
                              feature_vector_dict_file_name=feature_vector_dict_file,
                              dropout_vector_dict_file_name=dropout_vector_dict_file)
    end = time.clock()
    print "Runtime in seconds:", end - start
