import numpy
import pandas

# Preprocessing
from sklearn.preprocessing import Imputer

from sklearn.model_selection import GridSearchCV
# Models:
from sklearn.linear_model import LogisticRegression
from sklearn.svm import SVC
from sklearn.gaussian_process import GaussianProcessClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.neural_network import MLPClassifier

from classifiers.objects import ClassifierPool, ClassifierList


def train(df, model, config):
    week_ahead = config['week_ahead']['value']
    nweek = max(df['feature_week'].unique())
    users = df['user_id'].unique()
    classifiers = []
    weeks = []
    for week in range(1, nweek+1):
        feature = df.loc[df['feature_week'] == week, df.columns.drop(['feature_week', 'dropout'])]
        dropout = df.loc[df['feature_week'] == week+week_ahead, ['user_id', 'dropout']]
        dropout.dropna(subset=['dropout'], inplace=True)
        wdf = pandas.merge(feature, dropout, on=['user_id'], how='right')
        feature = df.loc[:, wdf.columns.drop(['user_id', 'dropout'])]
        dropout = df.loc[:, 'dropout']

        # If only one class of dropout value, Continue
        if len(dropout.unique()) == 1:
            continue

        # Impute before fit
        imputer = Imputer(strategy='mean')
        feature = imputer.fit_transform(feature)
        dropout = dropout.as_matrix()

        # get model and fit
        classifier = get_model(model, config)
        classifier.fit(feature, dropout)
        classifiers.append(classifier)
        weeks.append((week, week + week_ahead))

    classifier_list = ClassifierList(classifiers, weeks)
    classifier_pool = ClassifierPool()
    classifier_pool.save(classifier_list)


def get_model(model, config):
    if model == 'logistic_regression':
        classifier = GridSearchCV(LogisticRegression(),
                                  param_grid=[{
                                      'C': [1],
                                      'fit_intercept': [True],
                                      'solver': ['newton-cg']
                                  }])
    elif model == 'svm':
        classifier = GridSearchCV(SVC(),
                                  param_grid=[{
                                      'C': [1, 10, 100, 1000],
                                      'kernel': ['linear', 'poly', 'rbf'],
                                      'degree': [3, 4, 5],
                                      'probability': [True, False],
                                      'shrinking': [True, False]}]
                                  )
    elif model == 'gaussian_process':
        classifier = GridSearchCV(GaussianProcessClassifier(),
                                  param_grid=[{'optimizer': [None, 'fmin_l_bfgs_b'],
                                               'n_restarts_optimizer': [0, 1, 2]
                                               }])
    elif model == 'decision_tree':
        classifier = GridSearchCV(DecisionTreeClassifier(),
                                  param_grid=[{'criterion': ['gini', 'entropy'],
                                               'max_features': ['auto', 'sqrt', 'log2', None]
                                               }])
    elif model == 'random_forest':
        classifier = GridSearchCV(RandomForestClassifier(),
                                  param_grid=[{'n_estimators': [10, 20, 30],
                                               'criterion': ['gini', 'entropy'],
                                               'max_features': ['sqrt', 'log2', None],
                                               'bootstrap': [True, False]
                                               }])
    elif model == 'neural_network':
        classifier = GridSearchCV(MLPClassifier(),
                                  param_grid=[{'activation': ['identity', 'logistic', 'tanh', 'relu'],
                                               'solver': ['sgd', 'adam'],
                                               'learning_rate': ['constant', 'invscaling', 'adaptive'],
                                               'shuffle': [True, False]
                                               }])
    else:
        raise ValueError("Invalid model type")

    return classifier
