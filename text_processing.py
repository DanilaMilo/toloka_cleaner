import json
import re
from typing import List

import pandas as pd
from pymystem3 import Mystem
from spellchecker import SpellChecker

def read_table(filename):
    if '.csv' in filename:
        df = pd.read_csv(filename)
    if '.xls' in filename or '.od' in filename:
        df = pd.read_excel(filename)
    return df

class Redactor:

    def __init__(self):
        self.stemmer = Mystem()
        self.speller = SpellChecker(language='ru')
        self.stopwords = json.load(open('sources/stopwords.json')).get('words')

    def processing_pipeline(self, path_to_csv: str, target_main: List[str] = [],
                            target_action: List[str] = [], table_name: str = 'TEXT', max_text_len=75) -> pd.DataFrame:
        data = pd.read_csv(path_to_csv)
        if table_name not in data.columns:
            return False
        data.fillna('',inplace=True)
        target_main, target_action = self.lemm_targets(target_main, target_action)
        data = self.clean_text(data, table_name)
        data = self.lemm_rows(data, table_name)
        data = self.filtering_text(data, target_main, target_action)
        data = self.clean_trash_text(data, table_name, max_text_len)
        return data

    def generate_ngrams(self, path_to_csv: str, table_name: str = 'TEXT') -> List[str]:
        def get_ngram(text: List[str], grams: int, ignore: str) -> List[List[str]]:
            """
            :param text: Separated sentence for extract ngrams
            :param grams: Number of words in gram
            :param ignore: Part of speech to ignore (ex. ADV, S)
            :return: [[ngram_1], ... [ngram_i]]
            """
            model = []
            # model will contain n-gram strings
            count = 0
            for token in text[:len(text) - grams + 1]:
                gram = text[count:count + grams]
                try:
                    # Дублируется условие из-за неожиданной работы метода analyze
                    if all(self.stemmer.analyze(word)[0]['analysis'][0]['gr'].split('=')[0] != ignore for word in gram) and \
                                all(self.stemmer.analyze(word)[0]['analysis'][0]['gr'].split(',')[0] != ignore for word in
                                    gram):
                        if text[count:count + grams] not in model:
                            model.append((text[count:count + grams]))
                except:
                    if text[count:count + grams] not in model:
                        model.append((text[count:count + grams]))

                count = count + 1
            return model

        data = pd.read_csv(path_to_csv)
        if table_name not in data.columns:
            return False
        data.fillna('', inplace=True)
        data = self.lemm_rows(data, table_name=table_name)
        data = self.clean_text(data, table_name="lemms")
        lemm_text = data['lemms']
        lemm_clean_text = []
        # Remove stopwords
        for seq in lemm_text:
            for word in seq:
                if word in self.stopwords:
                    seq.replace(word, '')
            lemm_clean_text.append(seq)
        # Generate 2-grams & 3-grams with ADV,S,A,ADVPRO and save
        two_grams_main_count = {}
        two_grams_action_count = {}
        three_grams_main_count = {}
        three_grams_action_count = {}
        # Pizdec region
        for text in lemm_clean_text:
            # Для main игнорируем глаголы, для action существительные
            splitted_text = text.split()
            two_grams_main = get_ngram(splitted_text, 2, 'V')
            three_grams_main = get_ngram(splitted_text, 3, 'V')
            two_grams_action = get_ngram(splitted_text, 2, 'S')
            three_grams_action = get_ngram(splitted_text, 3, 'S')
            for seq in two_grams_main:
                if ' '.join(seq) not in two_grams_main_count:
                    two_grams_main_count[' '.join(seq)] = 0
                two_grams_main_count[' '.join(seq)] += 1
            for seq in three_grams_main:
                if ' '.join(seq) not in three_grams_main_count:
                    three_grams_main_count[' '.join(seq)] = 0
                three_grams_main_count[' '.join(seq)] += 1
            for seq in two_grams_action:
                if ' '.join(seq) not in two_grams_action_count:
                    two_grams_action_count[' '.join(seq)] = 0
                two_grams_action_count[' '.join(seq)] += 1
            for seq in three_grams_action:
                if ' '.join(seq) not in three_grams_action_count:
                    three_grams_action_count[' '.join(seq)] = 0
                three_grams_action_count[' '.join(seq)] += 1
        top_two_grams_main_sorted = list(
            dict(sorted(two_grams_main_count.items(), key=lambda item: item[1], reverse=True)).keys())
        if len(top_two_grams_main_sorted) < 15:
            top_grams_main = top_two_grams_main_sorted
        else:
            top_grams_main = top_two_grams_main_sorted[:15]
        top_three_grams_main_sorted = list(
            dict(sorted(three_grams_main_count.items(), key=lambda item: item[1], reverse=True)).keys())
        if len(top_three_grams_main_sorted) < 15:
            top_grams_main.extend(top_three_grams_main_sorted)
        else:
            top_grams_main.extend(top_three_grams_main_sorted[:15])
        top_two_grams_action_sorted = list(
            dict(sorted(two_grams_action_count.items(), key=lambda item: item[1], reverse=True)).keys())
        if len(top_two_grams_action_sorted) < 15:
            top_grams_action = top_two_grams_action_sorted
        else:
            top_grams_action = top_two_grams_action_sorted[:15]
        top_three_grams_action_sorted = list(
            dict(sorted(three_grams_action_count.items(), key=lambda item: item[1], reverse=True)).keys())
        if len(top_three_grams_action_sorted) < 15:
            top_grams_action.extend(top_three_grams_action_sorted)
        else:
            top_grams_action.extend(top_three_grams_action_sorted[:15])
        #Pizdec endregion
        return top_grams_main, top_grams_action

    def lemm_targets(self, target_main: List[str], target_action: List[str]) -> List[str]:
        lemm_target_main = [''.join(self.stemmer.lemmatize(t)[:-1]) for t in target_main]
        lemm_target_action = [''.join(self.stemmer.lemmatize(t)[:-1]) for t in target_action]
        return lemm_target_main, lemm_target_action

    def clean_text(self, data: pd.DataFrame, table_name: str) -> pd.DataFrame:
        texts_src = data[table_name]
        texts: List[str] = [re.sub('[^а-яё ]', ' ', str(t).lower()) for t in texts_src]
        texts = [re.sub(r" +", " ", t) for t in texts]
        data[table_name] = texts
        return data

    def lemm_rows(self, data: pd.DataFrame, table_name: str) -> pd.DataFrame:
        try:
            texts = data[table_name].values
        except:
            texts = data[table_name]
        texts_lemm = [''.join(self.stemmer.lemmatize(t)[:-1]) for t in texts]
        data['lemms'] = texts_lemm
        return data

    def filtering_text(self, data: pd.DataFrame, target_main, target_action) -> pd.DataFrame:
        texts_lemm = data['lemms']
        labels = []
        for t in texts_lemm:
            if len(target_action) > 0:
                if any(x in t for x in target_main) and any(x in t for x in target_action):
                    labels.append(1)
                else:
                    labels.append(0)
            else:
                if any(x in t for x in target_main):
                    labels.append(1)
                else:
                    labels.append(0)
        data['label'] = labels
        data.label.value_counts()
        return data

    def clean_trash_text(self, data: pd.DataFrame, table_name: str, max_text_len: int):
        # todo Убрать этот мусор
        unwanted_words_at_start: list = ["а", "ли", "я", "можно", "вот", "ok", "вообще", "вопрос"]
        unwanted_words: list = [
            "пожалуйста", "спасибо", "подскажите", "подскажи", "здравствуйте",
            "добрый вечер", "добрый день", "привет", "сбер", "афина", "джой",
            "алиса", "сири", "салют", "яндекс", "ассистент", "помощник",
            "ok", "google", "siri", "а ты знаешь", "а что будет если",
            "будь добр", "возможно ли", "вообще", "вот", "можно", "яндекс"
                                                                  "мне бы хотелось узнать", "бы", 'alexa', 'alex',
            'алекса', "гугл",
            "мне", "у меня", "я хочу узнать" "что ты знаешь", "бы хотел", "в принципе"]
        texts = data[data['label'] == 1][table_name].values
        clean_res_rows = []
        for text in texts:
            for item in unwanted_words:
                if item.lower() in text.split():
                    text = text.replace(item.lower(), '').replace('  ', ' ')
            temp_words = []
            for tok in text.split():
                if tok not in unwanted_words_at_start:
                    temp_words.append(tok)
            text = ' '.join(temp_words)
            if len(text) < int(max_text_len):
                clean_res_rows.append(re.sub(r'[^\w]', ' ', text))
        clean_res_rows = [re.sub(r" +", " ", t) for t in clean_res_rows]
        data_clean = pd.DataFrame(clean_res_rows, columns=['text'])
        data_clean = data_clean.drop_duplicates(subset='text').reset_index(drop=True)
        return data_clean
