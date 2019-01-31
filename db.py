import pprint
import sqlparse
import sys
import re
# creating table structure
table_list_name = []
table_list = {}
aggregate_list = [ 'max(' , 'min(' , 'sum(' ,'average(']
columns_dictionary = {}
with open("metadata.txt") as f:
    content = f.read().splitlines()
flag=0
for i in range(len(content)):
	if(content[i-1] == '<begin_table>'):
		exec("%s = %s" % (content[i], {}))
		flag = 1
		current_dict = content[i]
		continue
	if content[i] == '<end_table>':
		flag = 0
		exec("table_list['%s']= %s " % (current_dict,current_dict))
		table_list_name.append(current_dict)
		# table_list.append(current_dict)
	if flag == 1: #populating the columns
		exec("%s['%s'] = %s" % (current_dict,content[i], []))
		exec("columns_dictionary['%s'] = []" % (content[i]))
		#print(current_dict)
# print(columns_dictionary)
# populating the tables

for i in table_list_name:
	table_file = i + '.csv'
	with open(table_file) as tf:
		content =tf.read().splitlines()
		for row in content:
			split_row = row.split(',')
			exec("table_name = %s" % (i))
			for k in range(len(table_name.keys())):
				list_keys = (list(table_name.keys()))
				table_name[list_keys[k]].append(int(split_row[k]))
			# print(table_name)
				# print (values)
# print(table_list['table2'])

# print(table2)
# for key,values in table1.items():
# 	print(values[0],)
def print_result(column_list,all_list):
	if(len(column_list)==0):
		print('Invalid Input')
		sys.exit()
	if(len(all_list) == 0):
		print('No Matching Rows')
		sys.exit()
	for i in range(len(column_list)):# names of columns printed by this for loop
		if(i == len(column_list)-1 ):
			print('%s'%(column_list[i]),end ="")
		else:
			print('%s,'%(column_list[i]),end ="")
	print()

	for row in all_list: # rows
		for i in range(len(row)):
			if(i ==  len(row)-1):
				print('%s'%row[i],end ="")
			else:
				print('%s,'%row[i],end ="")
		print()

def aggregate(i,table_name,column_name):

	if i == 0 :
		print('max('+table_name+'.'+column_name+')')
		print(max(table_list[table_name][column_name]))
	elif i == 1 :
		print('min('+table_name+'.'+column_name+')')
		print(min(table_list[table_name][column_name]))
	elif i == 2 :
		print('sum('+table_name+'.'+column_name+')')
		print(sum(table_list[table_name][column_name]))
	elif i == 3 :
		print('average('+table_name+'.'+column_name+')')
		print(sum(table_list[table_name][column_name])/len(table_list[table_name][column_name]))
	# elif function_name == 'min':
# print(table_list)

def cartesian(total_list):
	product = []
	complete_list = [e for e in total_list if e]
	for i in range(len(complete_list)):
		if(i == 0):
			product = complete_list[i]
		else:
			temp = []
			for j in range(len(product)):
				for k in complete_list[i]:
					temp.append(product[j] + k)
			product = temp
	return product
	
def select_multiple(columns,tables):
	column_list = []
	total_list = []
	for tab in tables:
		current_table = table_list[tab]
		number_of_rows=0
		if columns[0] == '*': 
			for i in range(len(current_table.keys())): # names of columns printed by this for loop
				number_of_rows = len(current_table[list(current_table.keys())[i]])
				column_list.append(tab+'.'+list(current_table.keys())[i])
			all_list = []
			for i in range(number_of_rows):
				tup = ()
				for j in range(len(current_table.keys())):
					tup+=(current_table[list(current_table.keys())[j]][i],)
				all_list.append(tup)
			total_list.append(all_list)

		else: # columns given
			flag = 0 
			for i in range(len(columns)):# names of columns printed by this for loop
				temp = columns[i]
				if '.' in columns[i]:
					if columns[i].split('.')[0] != tab:
						continue
					temp=columns[i].split('.')[1]
				if temp in current_table.keys():
					number_of_rows = len(current_table[temp])
					column_list.append(tab+'.'+temp)

			all_list = []
			for i in range(number_of_rows):
				tup = ()
				for j in range(len(columns)):
					temp=columns[j]
					if '.' in columns[j]:
						if columns[j].split('.')[0] != tab:
							continue
						temp=columns[j].split('.')[1]
					if temp in current_table.keys():
						tup+=(current_table[temp][i],)
				all_list.append(tup)
			total_list.append(all_list)
	product=cartesian(total_list)
	return column_list,product	
		
			# print_result(columns,all_list)

def select_distinct(columns,tables):
	tab=tables[0]
	current_table = table_list[tab]
	number_of_rows=0
	column_list=[]
	distinct_list=[]
	if columns[0] == '*':
		for i in range(len(current_table.keys())):
			number_of_rows = len(current_table[list(current_table.keys())[i]])
			column_list.append(tab+'.'+list(current_table.keys())[i])
			
		all_list = []
		for i in range(number_of_rows):
			tup = ()
			for j in range(len(current_table.keys())):
				tup+=(current_table[list(current_table.keys())[j]][i],)
			all_list.append(tup)
		
		for distinct in all_list:
			   if distinct not in distinct_list:
			       distinct_list.append(distinct)
			

	
	else:
		for i in range(len(columns)):
			temp = columns[i]
			if '.' in columns[i]:
				if columns[i].split('.')[0] != tab:
					continue
				temp=columns[i].split('.')[1]
			if temp in current_table.keys():
				number_of_rows = len(current_table[temp])
				column_list.append(tab+'.'+temp)
		
		all_list = []
		for i in range(number_of_rows):
			tup = ()
			for j in range(len(columns)):
				temp=columns[j]
				if '.' in columns[j]:
					if columns[j].split('.')[0] != tab:
						continue
					temp=columns[j].split('.')[1]
				if temp in current_table.keys():
					tup+=(current_table[temp][i],)
			all_list.append(tup)
		
		for distinct in all_list:
			   if distinct not in distinct_list:
			       distinct_list.append(distinct)

	return column_list,distinct_list

def select_join(columns,tables,key,operator,value):
	column_list = []
	total_list = []
	selected_columns=columns
	for tab in tables:
		current_table = table_list[tab]
		number_of_rows=0
		
		for i in range(len(current_table.keys())): # names of columns printed by this for loop
			number_of_rows = len(current_table[list(current_table.keys())[i]])
			column_list.append(tab+'.'+list(current_table.keys())[i])
			
		all_list = []
		for i in range(number_of_rows):
			tup = ()
			for j in range(len(current_table.keys())):
				tup+=(current_table[list(current_table.keys())[j]][i],)
			all_list.append(tup)
		total_list.append(all_list)
	product=cartesian(total_list)	
	# print(column_list)
	for tab in tables:
		current_table = table_list[tab]
		if columns[0] != '*':  # columns given
			for i in range(len(columns)):# names of columns printed by this for loop
				temp = columns[i]
				if '.' in columns[i]:
					if columns[i].split('.')[0] != tab:
						continue
					temp=columns[i].split('.')[1]
				if temp in current_table.keys():
					number_of_rows = len(current_table[temp])
					selected_columns[i]=(tab+'.'+temp)
	index1 = -9999
	index2 = -9999
	for i in range(len(column_list)):
		if(key == column_list[i]):
			index1 = i
		if(value == column_list[i]):
			index2 = i
	new_product=[]		
	# print_result(column_list,product)
	for j in range(len(product)): # for selecting rows
		if eval("%s%s%s" % (product[j][index1],operator,product[j][index2])):
			new_product.append(product[j])
	resultant_col=[]

	for i in range(len(selected_columns)):
		for j in range(len(column_list)):
			if(selected_columns[i]==column_list[j]):
				resultant_col.append(j)
	resultant_product=[]
	for j in new_product: # selecting the columns
		resultant_tuple=()
		for k in resultant_col:
			resultant_tuple+=(j[k],)
		resultant_product.append(resultant_tuple)
	if (columns[0]=='*'):
		return column_list,new_product
	return selected_columns,resultant_product	




def select_wh(columns,tables,keys,operators,values,logic=None):
	column_list = []
	total_list = []
	selected_columns=columns
	for tab in tables:
		current_table = table_list[tab]
		number_of_rows=0
		for i in range(len(current_table.keys())): # names of columns printed by this for loop
			number_of_rows = len(current_table[list(current_table.keys())[i]])
			column_list.append(tab+'.'+list(current_table.keys())[i])
			
		all_list = []
		for i in range(number_of_rows):
			tup = ()
			for j in range(len(current_table.keys())):
				tup+=(current_table[list(current_table.keys())[j]][i],)
			all_list.append(tup)
		total_list.append(all_list)
	product=cartesian(total_list)	
	# print(column_list)
	for tab in tables:
		current_table = table_list[tab]
		if columns[0] != '*':  # columns given
			for i in range(len(columns)):# names of columns printed by this for loop

				temp = columns[i]
				if '.' in columns[i]:
					if columns[i].split('.')[0] != tab:
						continue
					temp=columns[i].split('.')[1]
				if temp in current_table.keys():
					number_of_rows = len(current_table[temp])
					selected_columns[i]=(tab+'.'+temp)
	index1 = -9999
	index2 = -9999
	for i in range(len(column_list)):
		# print(col)
		if(keys[0] == column_list[i]):
			index1 = i
		if(len(keys) == 2 and keys[1] == column_list[i]):
			index2 = i
	new_product=[]		
	for j in range(len(product)): # selecting the rows according to given condition
		if(len(keys) == 1):
			if eval("%s%s%s" % (product[j][index1],operators[0],values[0])):
				new_product.append(product[j])
		elif(len(keys) == 2):
			if eval("%s%s%s %s %s%s%s" % (product[j][index1],operators[0],values[0],logic,product[j][index2],operators[1],values[1])):
				new_product.append(product[j])
	resultant_col=[]
	for i in range(len(selected_columns)):
		for j in range(len(column_list)):
			if(selected_columns[i]==column_list[j]):
				resultant_col.append(j)
	resultant_product=[]
	for j in new_product: # selecting the columns
		resultant_tuple=()
		for k in resultant_col:
			resultant_tuple+=(j[k],)
		resultant_product.append(resultant_tuple)
	if (columns[0]=='*'):
		return column_list,new_product
	return selected_columns,resultant_product	

# select_query(["A","B"],["table1"])
# print(max(table_list["table1"]['B']))
# select_wh(["A",'B','C','D'],["table1","table2"],['A','D'],['==','=='],[7,311],'and')

def parse_query(sql):
	parsed = sqlparse.parse(sql)
	stmt = parsed[0]
	str(stmt)
	return (stmt.tokens)

def validate_query(columns,tables):
	# print(table_list)
	for tab in tables:
		for key in table_list[tab].keys():
			columns_dictionary[key].append(tab)
	for col in columns:
		if col == '*':
			continue
		if '.' in col:
			continue
		if len(columns_dictionary[col]) > 1:
			print("Error : Column "+ col + " exists in more than one table")
			sys.exit()


def main():
	sql=sys.argv
	if ( len(sql) == 2):
		sql=str(sql[1])
		if sql[-1] != ';':
			print("Missing ; at the end")
			sys.exit()
		sql=sql[:-1]
		tokens = parse_query(sql)
		if(str(tokens[0]).lower() == 'select'):
			if (str(tokens[2]).lower() == 'distinct'):
				column_string = str(tokens[4]).replace(" ", "")
				columns = column_string.split(',')
				if (str(tokens[-1])[0:5] != 'where'):
					table_string = str(tokens[-1]).replace(" ", "")
					table_list = table_string.split(',')
					col,result=select_distinct(columns,table_list)
					print_result(col,result)
					sys.exit()
			else: # not contains distinct keyword
				column_string = str(tokens[2]).replace(" ", "")
				columns = column_string.split(',')
				if (str(tokens[-1])[0:5] != 'where'):
					table_string = str(tokens[-1]).replace(" ", "")
					table_list = table_string.split(',')
					s=columns[0]
					for i in range(len(aggregate_list)):
						if aggregate_list[i] in s:
							aggregate(i,table_list[0],s[s.find("(")+1:s.find(")")])
							sys.exit()
					
					validate_query(columns,table_list)
					col,result=select_multiple(columns,table_list)

					print_result(col,result)
					sys.exit()
				else: # contains where
					table_string = str(tokens[-3]).replace(" ", "")
					table_list = table_string.split(',')
					validate_query(columns,table_list)

					logic=None
					where_tokens = tokens[-1].tokens
					for i in range(len(where_tokens)):
						where_tokens[i]=str(where_tokens[i])
						where_tokens[i]=where_tokens[i].replace(" ", "")
					where_tokens.pop(0)
					where_tokens=[e for e in where_tokens if e]
					operators=[]
					keys=[]
					values=[]
					if 'and' in tokens[-1]:
						logic = 'and'
						for i in where_tokens:
							if len(re.findall("[<>=!]=?", i))>0:
								operators.append(re.findall("[<>=!]=?", i)[0])
								keys.append(re.split(r'[<>=!]=?',i)[0])
								values.append(re.split(r'[<>=!]=?',i)[1])
					elif 'or' in tokens[-1]:
						logic = 'or'
						for i in where_tokens:
							if len(re.findall("[<>=!]=?", i))>0:
								operators.append(re.findall("[<>=!]=?", i)[0])
								keys.append(re.split(r'[<>=!]=?',i)[0])
								values.append(re.split(r'[<>=!]=?',i)[1])
					else:
						if len(re.findall("[<>=!]=?", where_tokens[-1]))>0:
							operators.append(re.findall("[<>=!]=?", where_tokens[-1])[0])
							keys.append(re.split(r'[<>=!]=?',where_tokens[-1])[0])
							values.append(re.split(r'[<>=!]=?',where_tokens[-1])[1])
							if not values[0].isdigit():
								if(operators[0] == '='):
									operators[0] = '=='
								col,result=select_join(columns,table_list,keys[0],operators[0],values[0])
								print_result(col,result)
								sys.exit()
					for i in range(len(values)):
						if values[i].isdigit():
							values[i]=float(values[i])
					for i in range(len(keys)):
						if not '.' in keys[i]:
							keys[i]=columns_dictionary[keys[i]][0]+'.'+keys[i]

					for i in range(len(operators)):
						if(operators[i] == '='):
							operators[i]='=='
					col,result=select_wh(columns,table_list,keys,operators,values,logic)
					print_result(col,result)
					sys.exit()

	else:
		print('Invalid Input')	

main()
