import MySQLdb
import MySQLdb.cursors as cursors
import pandas

from data.objects import DataFrame, DataPool


class Connector(object):
    rep = None
    cfg_mysql = None
    conn = None

    def __init__(self, replacement):
        self.rep = replacement
        self.cfg_mysql = replacement

    def open_conn(self):
        self.conn = MySQLdb.connect(host=self.cfg_mysql['host'],
                                    port=self.cfg_mysql['port'],
                                    user=self.cfg_mysql['user'],
                                    passwd=self.cfg_mysql['password'],
                                    db=self.cfg_mysql['database'],
                                    cursorclass=cursors.SSCursor)

    def close_conn(self):
        self.conn.close()

    def get_feature_ids(self, selected_feature_table):
        cursor = self.conn.cursor()

        sql = "SELECT DISTINCT(feature_id) AS id, " \
              "(SELECT feature_name FROM {0}.feature_info WHERE {0}.feature_info.feature_id = id)" \
              "FROM {0}.{1} WHERE feature_id > 1" \
              .format(self.cfg_mysql['database'], selected_feature_table)
        cursor.execute(sql)
        data = cursor.fetchall()
        feature_id_frame = DataFrame(data, ['feature_id', 'feature_name'])
        cursor.close()
        self.conn.commit()
        return feature_id_frame

    def save_feature_frame(self, selected_feature_table, selected_feature_ids, selected_feature_names):
        frame_pool = DataPool()
        frames = []
        keys = ['user_id', 'feature_week']
        for i in range(len(selected_feature_ids)):
            cursor = self.conn.cursor()
            feature_description = '[{}] {}'.format(str(selected_feature_ids[i]).zfill(3), selected_feature_names[i])
            sql = "SELECT user_id, feature_week, feature_value FROM {0}.{1} WHERE feature_id = {2}" \
                  .format(self.cfg_mysql['database'], selected_feature_table, selected_feature_ids[i])
            cursor.execute(sql)
            data = cursor.fetchall()
            feature_frame = DataFrame(data, keys + [feature_description])
            frames.append(feature_frame)
            cursor.close()
        matched = frames[0]
        for i in range(1, len(frames)):
            matched = pandas.merge(matched, frames[i], on=keys, how='outer')
        matched = DataFrame(frame=matched)
        cursor = self.conn.cursor()
        sql = "SELECT user_id, feature_week, feature_value FROM {0}.{1} WHERE feature_id = 1" \
            .format(self.cfg_mysql['database'], selected_feature_table)
        cursor.execute(sql)
        data = cursor.fetchall()
        dropout_frame = DataFrame(data, keys + ['dropout'])
        cursor.close()
        matched = pandas.merge(matched, dropout_frame, on=keys, how='right')
        matched = DataFrame(frame=matched)
        frame_pool.save(matched)
        self.conn.commit()
        return True
