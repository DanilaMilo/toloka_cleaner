from flask import Flask, render_template, redirect, send_file
from text_processing import Redactor, read_table
import pandas as pd

app = Flask(__name__)



@app.route('/')
def index():
    return render_template('upload.html')


from flask import Flask, render_template, request
from werkzeug.utils import secure_filename

app = Flask(__name__)
redactor = Redactor()


@app.route('/')
def upload_file():
    return render_template('upload.html')

@app.route('/download', methods = ["POST", "GET"])
def download_file():
    try:
        return send_file(request.values['file_to_download'])
    except:
        print("Произошла ошибка")

@app.route('/upload_ngram', methods=['GET', 'POST'])
def uploader_file_ngram():
    # Взять дополнительные target_main, target_action из request.values, где 'on'
    filename = request.values['filename']
    max_len = request.values['max_text_len']
    target_main = request.values.get('target_main', '').replace("\xa0"," ").split(',')
    target_action = request.values.get('target_action', '').replace("\xa0"," ").split(',')
    column_name = request.values.get('column_name', 'TEXT')
    for val in request.values:
        if request.values[val] == 'on':
            if val[-5:] == '_main':
                target_main.append(val.replace('_main',''))
            elif val[-7:] == '_action':
                target_action.append(val.replace('_action',''))
    clean_df = redactor.processing_pipeline(filename, max_text_len=max_len, target_main=target_main,
                                            table_name=column_name)
    new_filename = filename.replace('.csv', '_clean.csv').replace(' ', '_')
    clean_df.to_csv(new_filename, index=False)
    return render_template('download.html', filename=new_filename, column_name=column_name,
                           target_main=','.join(target_main).replace(' ', '\xa0'),
                           target_action=','.join(target_action).replace(' ', '\xa0'))


@app.route('/upload', methods=['GET', 'POST'])
def uploader_file():
    if request.method == 'POST':
        # Get values
        max_len = request.values['max_text_len']
        target_main = request.values.get('target_main','').split(',')
        target_action = request.values.get('target_action','').split(',')
        column_name = request.values.get('column_name', 'TEXT')
        # Save file
        f = request.files['file']
        f.save(f.filename)
        if column_name not in read_table(f.filename).columns:
            return render_template('upload.html')
        if request.values.get('generate_ngrams'):
            addict_main, addict_action = redactor.generate_ngrams(path_to_csv=f.filename, table_name=column_name)
            return render_template('ngram_choice.html', filename=f.filename, max_text_len=max_len,
                                   target_main=','.join(target_main).replace(' ', '\xa0'),
                                   target_action=','.join(target_action).replace(' ', '\xa0'), column_name=column_name,
                                   ngrams_main=addict_main, ngrams_action=addict_action)
        else:
            # Text clean pipeline
            clean_df = redactor.processing_pipeline(f.filename, max_text_len=max_len, target_main=target_main,
                                                    table_name=column_name)
            # Change filename
            new_filename = f.filename.replace('.csv','_clean.csv').replace(' ', '_')
            # save cleaned file
            clean_df.to_csv(new_filename, index = False)
            return render_template('download.html', filename = new_filename, column_name=column_name,
                                   target_main=','.join(target_main).replace(' ', '\xa0'),
                                   target_action=','.join(target_action).replace(' ', '\xa0'))

if __name__ == '__main__':
    app.run(debug=True)